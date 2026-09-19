package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/index/graph"
	"github.com/impire-io/chronicle/internal/index/search"
	"github.com/impire-io/chronicle/internal/index/semantic"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/node"
)

// InProcess is backend zero (06-scheduler.md § backends): the actuator
// that runs placements as goroutines in this process, absorbing item 29's
// scheduler-less watcher. It lands with the loop itself and keeps the
// actuator seam honest — if the seam cannot express "start a goroutine",
// the seam is wrong. It is not a 0004 backend: it isolates nothing, and
// microsandbox follows as its own increment behind the same interface.
type InProcess struct {
	// URL is the NATS URL placements dial with their pulled credentials.
	URL string
	// Creds pulls a workload's service creds — the record-verified pull
	// against chronicle-control, retried by the backend because the assign
	// may still be landing when the placement starts.
	Creds func(ctx context.Context, tenant, workload string) ([]byte, error)
	// Embedding is the install's provider (0016); nil means no provider,
	// and this host does not bid for semantic workloads.
	Embedding *semantic.ProviderConfig
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// Name is the backend's roster identity.
func (b *InProcess) Name() string { return "inprocess" }

// Supports names this host's vocabulary: semantic only with a provider.
func (b *InProcess) Supports(kind string) bool {
	switch kind {
	case contract.WorkloadKindNode, contract.WorkloadKindIndexSearch, contract.WorkloadKindIndexGraph:
		return true
	case contract.WorkloadKindIndexSemantic:
		return b.Embedding.Configured()
	}
	return false
}

// Start runs one placement: pull the creds (retrying across the
// assign-to-fold lag), connect as the tenant's service user — the only
// credential a placement ever holds — and start the workload kind.
func (b *InProcess) Start(ctx context.Context, spec Spec) (Placement, error) {
	logger := b.Logger
	if logger == nil {
		logger = slog.Default()
	}
	creds, err := pullCredsWithRetry(ctx, b.Creds, spec.Tenant, spec.Workload)
	if err != nil {
		return nil, fmt.Errorf("pull creds for %s/%s: %w", spec.Tenant, spec.Workload, err)
	}
	nc, err := mint.ConnectCreds(b.URL, creds, "chronicle-"+spec.Kind+"-"+spec.Tenant)
	if err != nil {
		return nil, fmt.Errorf("connect placement: %w", err)
	}

	done := make(chan struct{})
	nc.SetClosedHandler(func(*nats.Conn) { close(done) })

	startCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var stop func()
	switch spec.Kind {
	case contract.WorkloadKindNode:
		// The node's reports ride its default bridge transport — the same
		// path whether the placement is a goroutine or a microVM.
		n, err := node.Start(startCtx, nc, node.Config{Logger: logger})
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("start node: %w", err)
		}
		stop = n.Stop
	case contract.WorkloadKindIndexSearch:
		svc, err := search.Start(startCtx, nc, search.Config{Log: spec.Log, Index: spec.Index, Logger: logger})
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("start indexer: %w", err)
		}
		stop = svc.Stop
	case contract.WorkloadKindIndexGraph:
		svc, err := graph.Start(startCtx, nc, graph.Config{Log: spec.Log, Index: spec.Index, Logger: logger})
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("start graph indexer: %w", err)
		}
		stop = svc.Stop
	case contract.WorkloadKindIndexSemantic:
		if !b.Embedding.Configured() {
			nc.Close()
			return nil, fmt.Errorf("no embedding provider on this host")
		}
		svc, err := semantic.Start(startCtx, nc, semantic.Config{Log: spec.Log, Index: spec.Index, Provider: *b.Embedding, Logger: logger})
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("start semantic indexer: %w", err)
		}
		stop = svc.Stop
	default:
		nc.Close()
		return nil, fmt.Errorf("kind %q outside backend zero's vocabulary", spec.Kind)
	}
	return &inProcessPlacement{stop: stop, nc: nc, done: done}, nil
}

type inProcessPlacement struct {
	stop func()
	nc   *nats.Conn
	done chan struct{}
}

func (p *inProcessPlacement) Stop() {
	p.stop()
	p.nc.Close()
}

func (p *inProcessPlacement) Done() <-chan struct{} { return p.done }

// PullCreds is the request the backend's Creds hook normally wraps: ask
// chronicle-control for an assigned workload's service creds over the
// executor's own connection.
func PullCreds(ctx context.Context, nc *nats.Conn, executorID, tenant, workload string) ([]byte, error) {
	data, err := json.Marshal(contract.FleetCredsRequest{Executor: executorID, Tenant: tenant, Workload: workload})
	if err != nil {
		return nil, err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	msg, err := nc.RequestWithContext(reqCtx, contract.FleetCredsSubject(executorID), data)
	if err != nil {
		return nil, err
	}
	if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
		return nil, fmt.Errorf("%s: %s", code, msg.Header.Get(micro.ErrorHeader))
	}
	var resp contract.FleetCredsResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	if len(resp.Creds) == 0 {
		return nil, errors.New("empty creds reply")
	}
	return resp.Creds, nil
}
