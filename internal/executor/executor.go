// Package executor runs chronicle-executor: the muscle of decision 0014's
// fleet — one per host, deliberately dumb. It bids on auctions from live
// local state, runs what it is delegated through the ensure/status/destroy
// actuator seam, restarts its own placements locally with backoff (custody
// unchanged — no log traffic), and reports when a restart budget is
// exhausted. It holds no fleet-log access: its custody events are
// authenticated requests the workload service turns into ops.
package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/internal/workloads"
)

// Spec is one placement's identity and boot configuration — the delegation
// made local.
type Spec struct {
	Tenant   string
	Workload string
	Slot     string
	Kind     string
	Log      string
	Index    string
}

// Placement is one running instance of a workload on this host's backend.
type Placement interface {
	// Stop tears the placement down. Idempotent.
	Stop()
	// Done is closed when the placement dies on its own — the executor's
	// cue to restart locally within the budget.
	Done() <-chan struct{}
}

// Backend starts placements — the actuator beneath the seam. Backend zero
// is the in-process actuator; microsandbox is the first 0004 backend
// behind this same interface.
type Backend interface {
	// Name is the backend's roster identity ("inprocess", "microsandbox").
	Name() string
	// Supports reports whether this host can carry the kind — the bid's
	// capability half. A semantic workload needs the install's embedding
	// provider; a host without one stays honestly silent at auction.
	Supports(kind string) bool
	// Start runs one placement attempt. An error is a failed attempt,
	// counted against the restart budget.
	Start(ctx context.Context, spec Spec) (Placement, error)
}

// Config wires one executor.
type Config struct {
	// ID is the executor's durable identity — stable across restarts, one
	// per host.
	ID string
	// Backend is this host's one configured actuator.
	Backend Backend
	// Logger; nil means slog.Default.
	Logger *slog.Logger
	// MaxRestarts is the local restart budget per placement before the
	// slot is reported released. Zero means DefaultMaxRestarts.
	MaxRestarts int
}

// DefaultMaxRestarts is the local budget: blunt restarts are safe because
// workloads are crash-only, but hammering a placement that keeps dying
// helps nobody — the record learns instead.
const DefaultMaxRestarts = 3

// Executor is one running chronicle-executor.
type Executor struct {
	svc     micro.Service
	auction *nats.Subscription
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	e       *executor
}

// Stop stops the endpoints and every local placement.
func (x *Executor) Stop() {
	_ = x.auction.Unsubscribe()
	_ = x.svc.Stop()
	x.cancel()
	x.e.mu.Lock()
	placements := x.e.placements
	x.e.placements = map[string]*run{}
	x.e.mu.Unlock()
	for _, r := range placements {
		r.stop()
	}
	x.wg.Wait()
}

type executor struct {
	id          string
	nc          *nats.Conn
	backend     Backend
	logger      *slog.Logger
	maxRestarts int

	ctx context.Context
	wg  *sync.WaitGroup

	mu         sync.Mutex
	placements map[string]*run
}

// run is one placement's local supervision.
type run struct {
	spec   Spec
	cancel context.CancelFunc

	mu        sync.Mutex
	status    string
	placement Placement
	stopped   bool
}

func (r *run) stop() {
	r.cancel()
	r.mu.Lock()
	r.stopped = true
	p := r.placement
	r.placement = nil
	r.mu.Unlock()
	if p != nil {
		p.Stop()
	}
}

func key(tenant, workload string) string { return tenant + "/" + workload }

// Start registers the executor on the roster (retrying until a workload
// service answers — at boot the two race), subscribes the auction, and
// serves the per-executor endpoints. Stopping the returned executor is the
// caller's job.
func Start(ctx context.Context, nc *nats.Conn, cfg Config) (*Executor, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("executor needs a durable ID")
	}
	if err := contract.ValidateExecutorName(cfg.ID); err != nil {
		return nil, err
	}
	if cfg.Backend == nil {
		return nil, fmt.Errorf("executor needs its one configured backend")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	maxRestarts := cfg.MaxRestarts
	if maxRestarts == 0 {
		maxRestarts = DefaultMaxRestarts
	}

	runCtx, cancel := context.WithCancel(context.Background())
	e := &executor{
		id:          cfg.ID,
		nc:          nc,
		backend:     cfg.Backend,
		logger:      logger,
		maxRestarts: maxRestarts,
		ctx:         runCtx,
		placements:  map[string]*run{},
	}

	// The roster record: re-registering a known executor is the normal
	// fresh-boot case and idempotent by the birth guard.
	registered := false
	for attempt := 0; attempt < 20; attempt++ {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := workloads.Register(reqCtx, nc, contract.FleetRegisterRequest{Executor: cfg.ID, Backend: cfg.Backend.Name()})
		reqCancel()
		if err == nil {
			registered = true
			break
		}
		select {
		case <-ctx.Done():
			cancel()
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if !registered {
		cancel()
		return nil, fmt.Errorf("register executor %s: no workload service answered", cfg.ID)
	}

	x := &Executor{cancel: cancel, e: e}
	e.wg = &x.wg

	svc, err := micro.AddService(nc, micro.Config{
		Name:        "chronicle-executor",
		Version:     version.Version,
		Description: "chronicle fleet executor: bid, run delegations, report — backend " + cfg.Backend.Name(),
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("register service: %w", err)
	}
	endpoints := []struct {
		name    string
		subject string
		handler micro.HandlerFunc
	}{
		{"fleet-delegate", contract.FleetDelegateSubject(cfg.ID), e.handleDelegate},
		{"fleet-status", contract.FleetStatusSubject(cfg.ID), e.handleStatus},
		{"fleet-destroy", contract.FleetDestroySubject(cfg.ID), e.handleDestroy},
	}
	for _, ep := range endpoints {
		if err := svc.AddEndpoint(ep.name, ep.handler, micro.WithEndpointSubject(ep.subject)); err != nil {
			_ = svc.Stop()
			cancel()
			return nil, fmt.Errorf("add %s endpoint: %w", ep.name, err)
		}
	}

	// The auction is a plain subscription, deliberately not a queue group:
	// every executor must hear the scatter, and each replies for itself.
	sub, err := nc.Subscribe(contract.FleetAuctionSubject, e.handleAuction)
	if err != nil {
		_ = svc.Stop()
		cancel()
		return nil, fmt.Errorf("subscribe auction: %w", err)
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = sub.Unsubscribe()
		_ = svc.Stop()
		cancel()
		return nil, fmt.Errorf("flush subscriptions: %w", err)
	}
	x.svc = svc
	x.auction = sub
	return x, nil
}

// handleAuction bids from live local state: replying at all is the
// capability claim, the placement count is the honest load score.
func (e *executor) handleAuction(msg *nats.Msg) {
	if msg.Reply == "" {
		return
	}
	var r contract.FleetAuctionRequest
	if err := json.Unmarshal(msg.Data, &r); err != nil {
		return
	}
	if !e.backend.Supports(r.Kind) {
		return // silence is the honest non-bid
	}
	e.mu.Lock()
	load := len(e.placements)
	e.mu.Unlock()
	bid, err := json.Marshal(contract.FleetAuctionBid{Executor: e.id, Placements: load})
	if err != nil {
		return
	}
	_ = msg.Respond(bid)
}

// handleDelegate accepts or refuses one slot. On accept the placement
// starts optimistically — the guard settles who won, and a lost race
// arrives as a destroy.
func (e *executor) handleDelegate(req micro.Request) {
	var r contract.FleetDelegateRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	refuse := func(reason string) {
		reply, _ := json.Marshal(contract.FleetDelegateResponse{Accepted: false, Reason: reason})
		_ = req.Respond(reply)
	}
	if !e.backend.Supports(r.Kind) {
		refuse("kind outside this backend's vocabulary")
		return
	}

	spec := Spec{Tenant: r.Tenant, Workload: r.Workload, Slot: r.Slot, Kind: r.Kind, Log: r.Log, Index: r.Index}
	k := key(r.Tenant, r.Workload)
	e.mu.Lock()
	if _, exists := e.placements[k]; exists {
		e.mu.Unlock()
		refuse(contract.RefusalAlreadyCarrying)
		return
	}
	runCtx, cancel := context.WithCancel(e.ctx)
	pr := &run{spec: spec, cancel: cancel, status: contract.PlacementStarting}
	e.placements[k] = pr
	e.mu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.supervise(runCtx, pr)
	}()

	reply, err := json.Marshal(contract.FleetDelegateResponse{Accepted: true})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// supervise runs one placement with the local restart budget: start,
// watch, restart with backoff — silently, custody unchanged — and when the
// budget is spent, remove the placement and report the release. The report
// puts the failure on the record; the level scan re-auctions.
func (e *executor) supervise(ctx context.Context, pr *run) {
	k := key(pr.spec.Tenant, pr.spec.Workload)
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < e.maxRestarts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		p, err := e.backend.Start(ctx, pr.spec)
		if err != nil {
			e.logger.Warn("executor: placement start failed", "executor", e.id, "workload", k, "attempt", attempt+1, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			continue
		}
		pr.mu.Lock()
		if pr.stopped {
			pr.mu.Unlock()
			p.Stop()
			return
		}
		pr.placement = p
		pr.status = contract.PlacementRunning
		pr.mu.Unlock()
		e.logger.Info("executor: placement running", "executor", e.id, "workload", k, "kind", pr.spec.Kind)

		select {
		case <-ctx.Done():
			return
		case <-p.Done():
			// Died on its own; crash-only workloads owe the corpse nothing.
			pr.mu.Lock()
			pr.placement = nil
			pr.status = contract.PlacementStarting
			pr.mu.Unlock()
			e.logger.Warn("executor: placement died; restarting locally", "executor", e.id, "workload", k, "attempt", attempt+1)
		}
	}

	// Budget spent: the slot opens on the record, with the reason.
	e.mu.Lock()
	delete(e.placements, k)
	e.mu.Unlock()
	pr.stop()
	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := workloads.Report(reqCtx, e.nc, contract.FleetReportRequest{
		Executor: e.id,
		Tenant:   pr.spec.Tenant,
		Workload: pr.spec.Workload,
		Slot:     pr.spec.Slot,
		Reason:   contract.ReleaseFailing,
	}); err != nil {
		// The scan's witnesses catch what the report could not say.
		e.logger.Warn("executor: failing report unanswered", "executor", e.id, "workload", k, "err", err)
	}
}

// handleStatus is the backend witness: what does this host actually run?
func (e *executor) handleStatus(req micro.Request) {
	var r contract.FleetStatusRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	status := contract.PlacementNotFound
	e.mu.Lock()
	if pr, ok := e.placements[key(r.Tenant, r.Workload)]; ok {
		pr.mu.Lock()
		status = pr.status
		pr.mu.Unlock()
	}
	e.mu.Unlock()
	reply, err := json.Marshal(contract.FleetStatusResponse{Status: status})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// handleDestroy converges toward absence; destroying the absent is
// success.
func (e *executor) handleDestroy(req micro.Request) {
	var r contract.FleetDestroyRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	e.mu.Lock()
	pr := e.placements[key(r.Tenant, r.Workload)]
	delete(e.placements, key(r.Tenant, r.Workload))
	e.mu.Unlock()
	if pr != nil {
		pr.stop()
		e.logger.Info("executor: placement destroyed", "executor", e.id, "tenant", r.Tenant, "workload", r.Workload)
	}
	reply, err := json.Marshal(contract.FleetDestroyResponse{Destroyed: true})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}
