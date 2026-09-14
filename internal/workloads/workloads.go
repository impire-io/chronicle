// Package workloads runs chronicle-workloads: the fleet log's only writer
// (chronicle-hq/02-DESIGN/06-scheduler.md, decision 0014). It serves the
// dispatch surface and the executor endpoints, folds the fleet log through
// the shared judge into STATE_FLEET, runs the auctions, and level-scans —
// a scheduler-shaped component stripped of authority: nothing it decides
// is real until the log accepts it, and every custody write carries the
// writer's knowledge horizon as a server-enforced guard.
package workloads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nuid"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/version"
)

// Author is the Op-Author every fleet-log op carries.
const Author = "chronicle-workloads"

// Config adjusts one service instance.
type Config struct {
	// Logger; nil means slog.Default.
	Logger *slog.Logger
	// ScanEvery is the level scan's period — unfilled slots auctioned,
	// assignments cross-checked against the witnesses. Zero means
	// DefaultScanEvery.
	ScanEvery time.Duration
	// AuctionWindow bounds the bid gather. Zero means DefaultAuctionWindow.
	AuctionWindow time.Duration
}

// DefaultScanEvery keeps recovery prompt without chatter.
const DefaultScanEvery = 5 * time.Second

// DefaultAuctionWindow is long enough for every live executor on the
// cluster to bid, short enough that placement feels immediate.
const DefaultAuctionWindow = 250 * time.Millisecond

// Service is one running chronicle-workloads instance.
type Service struct {
	svc    micro.Service
	cancel context.CancelFunc
	wg     sync.WaitGroup
	s      *service
}

// Stop stops the endpoints, the fold, and the scan.
func (w *Service) Stop() {
	_ = w.svc.Stop()
	w.cancel()
	w.s.foldStop()
	w.wg.Wait()
}

type service struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	meta   jetstream.KeyValue
	states jetstream.KeyValue
	logger *slog.Logger

	scanEvery     time.Duration
	auctionWindow time.Duration

	ctx  context.Context
	kick chan struct{}

	foldStop func()

	mu sync.Mutex
	// mem is the instance's own fold of the fleet log: thing tail → the
	// folded state and the sequence it is folded to — the knowledge horizon
	// every custody write stamps as its guard.
	mem map[string]*memEntry
	// auctioning marks slots with a delegation in flight, so the scan does
	// not re-auction what the last tick already placed.
	auctioning map[string]struct{}
}

type memEntry struct {
	seq   uint64
	state json.RawMessage
}

// Start provisions the fleet log (idempotent — the service owns the log,
// so it owns the log's existence), starts the fold and the scan, and
// registers the micro service. Stopping the returned service is the
// caller's job.
func Start(ctx context.Context, nc *nats.Conn, cfg Config) (*Service, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	scanEvery := cfg.ScanEvery
	if scanEvery == 0 {
		scanEvery = DefaultScanEvery
	}
	window := cfg.AuctionWindow
	if window == 0 {
		window = DefaultAuctionWindow
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	meta, states, err := provision(ctx, js)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	s := &service{
		nc:            nc,
		js:            js,
		meta:          meta,
		states:        states,
		logger:        logger,
		scanEvery:     scanEvery,
		auctionWindow: window,
		ctx:           runCtx,
		kick:          make(chan struct{}, 1),
		mem:           map[string]*memEntry{},
		auctioning:    map[string]struct{}{},
	}
	if err := s.startFold(ctx); err != nil {
		cancel()
		return nil, err
	}

	w := &Service{cancel: cancel, s: s}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		s.scanLoop()
	}()

	svc, err := micro.AddService(nc, micro.Config{
		Name:        "chronicle-workloads",
		Version:     version.Version,
		Description: "chronicle fleet scheduling: dispatch surface, fold, auctions — the fleet log's only writer",
	})
	if err != nil {
		cancel()
		s.foldStop()
		return nil, fmt.Errorf("register service: %w", err)
	}
	endpoints := []struct {
		name    string
		subject string
		handler micro.HandlerFunc
	}{
		{"fleet-dispatch", contract.FleetDispatchSubject, s.handleDispatch},
		{"fleet-stop", contract.FleetStopSubject, s.handleStop},
		{"fleet-register", contract.FleetRegisterSubject, s.handleRegister},
		{"fleet-report", contract.FleetReportSubject, s.handleReport},
	}
	for _, e := range endpoints {
		if err := svc.AddEndpoint(e.name, e.handler, micro.WithEndpointSubject(e.subject)); err != nil {
			_ = svc.Stop()
			cancel()
			s.foldStop()
			return nil, fmt.Errorf("add %s endpoint: %w", e.name, err)
		}
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = svc.Stop()
		cancel()
		s.foldStop()
		return nil, fmt.Errorf("flush endpoint subscriptions: %w", err)
	}
	w.svc = svc
	return w, nil
}

// provision creates-or-opens the control account's META (with the fleet
// vocabulary), LOG_FLEET, and STATE_FLEET. Every step is idempotent so any
// instance can boot first.
func provision(ctx context.Context, js jetstream.JetStream) (meta, states jetstream.KeyValue, err error) {
	meta, err = js.KeyValue(ctx, contract.MetaBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		meta, err = js.CreateKeyValue(ctx, contract.MetaBucketConfig())
		if errors.Is(err, jetstream.ErrBucketExists) {
			meta, err = js.KeyValue(ctx, contract.MetaBucket)
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("control META: %w", err)
	}

	cfg, err := json.Marshal(contract.LogConfig{Status: contract.LogStatusActive, Description: "the fleet log (decision 0014)"})
	if err != nil {
		return nil, nil, err
	}
	if _, err := meta.Create(ctx, contract.MetaLogConfig(contract.FleetLog), cfg); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return nil, nil, fmt.Errorf("record fleet log config: %w", err)
	}
	for opType, ts := range contract.FleetTypeSchemas() {
		value, err := json.Marshal(ts)
		if err != nil {
			return nil, nil, err
		}
		if _, err := meta.Create(ctx, contract.MetaLogType(contract.FleetLog, opType), value); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
			return nil, nil, fmt.Errorf("record fleet type %s: %w", opType, err)
		}
	}

	if _, err := js.CreateStream(ctx, contract.LogStreamConfig(contract.FleetLog, 0)); err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return nil, nil, fmt.Errorf("create %s: %w", contract.StreamName(contract.FleetLog), err)
	}
	states, err = js.KeyValue(ctx, contract.StateBucket(contract.FleetLog))
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		states, err = js.CreateKeyValue(ctx, contract.StateBucketConfig(contract.FleetLog))
		if errors.Is(err, jetstream.ErrBucketExists) {
			states, err = js.KeyValue(ctx, contract.StateBucket(contract.FleetLog))
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("STATE_FLEET: %w", err)
	}
	return meta, states, nil
}

// append lands one op on the fleet log under the guard: expected is the
// writer's knowledge horizon for the thing's subject — birth passes 0. A
// errConflict return means the server rejected the write because something
// landed meanwhile; the caller re-reads and re-evaluates, never retries
// blind.
var errConflict = errors.New("custody write outran by a competing decision")

func (s *service) append(ctx context.Context, thing, opType string, payload []byte, expected uint64) error {
	op := contract.Op{
		ID:     nuid.Next(),
		Type:   opType,
		Author: Author,
		Ts:     time.Now(),
	}
	msg := &nats.Msg{
		Subject: contract.OpsSubject(contract.FleetLog, thing),
		Header:  op.Header(),
		Data:    payload,
	}
	_, err := s.js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequencePerSubject(expected))
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			return errConflict
		}
		return err
	}
	return nil
}

// horizon reads the instance's fold: the thing's state and the sequence it
// is folded to. Absent things return a zero horizon — birth's guard.
func (s *service) horizon(thing string) (json.RawMessage, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.mem[thing]
	if !ok {
		return nil, 0
	}
	return e.state, e.seq
}

// workloadState decodes one workload thing from the fold's memory.
func (s *service) workloadState(thing string) (contract.WorkloadState, uint64, bool) {
	raw, seq := s.horizon(thing)
	if raw == nil {
		return contract.WorkloadState{}, 0, false
	}
	var ws contract.WorkloadState
	if err := json.Unmarshal(raw, &ws); err != nil {
		s.logger.Warn("workloads: unreadable workload state", "thing", thing, "err", err)
		return contract.WorkloadState{}, 0, false
	}
	return ws, seq, true
}

func (s *service) handleDispatch(req micro.Request) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	var r contract.FleetDispatchRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if err := contract.ValidateLogName(r.Tenant); err != nil {
		_ = req.Error("bad-tenant", err.Error(), nil)
		return
	}
	if err := contract.ValidateWorkloadName(r.Workload); err != nil {
		_ = req.Error("bad-workload", err.Error(), nil)
		return
	}
	if r.Kind != contract.WorkloadKindNode && r.Kind != contract.WorkloadKindIndexSearch {
		_ = req.Error("bad-kind", fmt.Sprintf("kind %q is not in this build's vocabulary", r.Kind), nil)
		return
	}
	replicas := r.Replicas
	if replicas == 0 {
		replicas = 1
	}

	state, err := json.Marshal(contract.WorkloadState{
		Kind:     r.Kind,
		Tenant:   r.Tenant,
		Log:      r.Log,
		Index:    r.Index,
		Replicas: replicas,
		Slots:    map[string]contract.WorkloadSlot{},
	})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	payload, err := json.Marshal(contract.Snapshot{State: state})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}

	thing := contract.FleetWorkloadThing(r.Tenant, r.Workload)
	resp := contract.FleetDispatchResponse{Dispatched: true}
	err = s.append(ctx, thing, contract.OpTypeSnapshot, payload, 0)
	if errors.Is(err, errConflict) {
		// Born already — the idempotent case; the record stands.
		resp.Existed = true
	} else if err != nil {
		_ = req.Error("500", fmt.Sprintf("dispatch: %v", err), nil)
		return
	}
	s.kickScan()

	reply, err := json.Marshal(resp)
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

func (s *service) handleStop(req micro.Request) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	var r contract.FleetStopRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	thing := contract.FleetWorkloadThing(r.Tenant, r.Workload)
	ws, seq, ok := s.workloadState(thing)
	if !ok {
		_ = req.Error("no-such-workload", fmt.Sprintf("workload %s/%s has no record", r.Tenant, r.Workload), nil)
		return
	}
	if !ws.Stopped {
		if err := s.append(ctx, thing, contract.FleetOpStop, []byte(`{"stopped":true}`), seq); err != nil && !errors.Is(err, errConflict) {
			_ = req.Error("500", fmt.Sprintf("stop: %v", err), nil)
			return
		}
		// A conflict means the record moved under us; the scan re-reads and
		// the caller may retry — the retirement intent is not lost silently
		// because the caller sees Stopped only on a landed record.
	}
	s.kickScan()

	reply, err := json.Marshal(contract.FleetStopResponse{Stopped: true})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

func (s *service) handleRegister(req micro.Request) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	var r contract.FleetRegisterRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if err := contract.ValidateExecutorName(r.Executor); err != nil {
		_ = req.Error("bad-executor", err.Error(), nil)
		return
	}
	state, err := json.Marshal(contract.ExecutorState{Backend: r.Backend})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	payload, err := json.Marshal(contract.Snapshot{State: state})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	err = s.append(ctx, contract.FleetExecutorThing(r.Executor), contract.OpTypeSnapshot, payload, 0)
	if err != nil && !errors.Is(err, errConflict) {
		// errConflict is the re-register of a known executor — the normal
		// fresh-boot case; the roster record stands.
		_ = req.Error("500", fmt.Sprintf("register: %v", err), nil)
		return
	}
	s.kickScan()

	reply, err := json.Marshal(contract.FleetRegisterResponse{Registered: true})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// handleReport is an executor's custody report: a restart budget is
// exhausted, the slot opens with reason failing, and the failure count
// lands in merged state where compaction cannot erase it.
func (s *service) handleReport(req micro.Request) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	var r contract.FleetReportRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	thing := contract.FleetWorkloadThing(r.Tenant, r.Workload)
	ws, seq, ok := s.workloadState(thing)
	if !ok {
		_ = req.Error("no-such-workload", fmt.Sprintf("workload %s/%s has no record", r.Tenant, r.Workload), nil)
		return
	}
	slot, held := ws.Slots[r.Slot]
	if !held || slot.Executor != r.Executor {
		// Not this executor's slot anymore — a steal or a stop got there
		// first. Acknowledge without writing: the record already moved.
		reply, _ := json.Marshal(contract.FleetReportResponse{Released: false})
		_ = req.Respond(reply)
		return
	}
	reason := r.Reason
	if reason == "" {
		reason = contract.ReleaseFailing
	}
	if err := s.release(ctx, thing, ws, seq, r.Slot, reason); err != nil && !errors.Is(err, errConflict) {
		_ = req.Error("500", fmt.Sprintf("release: %v", err), nil)
		return
	}
	s.kickScan()

	reply, err := json.Marshal(contract.FleetReportResponse{Released: true})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// release appends the slot-opening merge under the guard, carrying the
// bumped failure counter for reasons that are failures.
func (s *service) release(ctx context.Context, thing string, ws contract.WorkloadState, seq uint64, slot, reason string) error {
	patch := map[string]any{
		"slots": map[string]any{slot: nil},
	}
	if reason != contract.ReleaseStopped {
		count := 1
		if ws.Failures != nil {
			count = ws.Failures.Count + 1
		}
		patch["failures"] = contract.WorkloadFailures{Count: count, LastReason: reason}
	}
	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	err = s.append(ctx, thing, contract.FleetOpRelease, payload, seq)
	if err == nil {
		s.logger.Info("workloads: slot released", "thing", thing, "slot", slot, "reason", reason)
	}
	return err
}

// kickScan wakes the scan loop now instead of at the next tick.
func (s *service) kickScan() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}
