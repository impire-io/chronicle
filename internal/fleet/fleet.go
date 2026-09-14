// Package fleet composes `chronicle up`: the bootstrap NATS, control, one
// chronicle-workloads instance, and one embedded executor on the
// in-process backend — the fleet shape without the fleet ceremony
// (chronicle-hq/02-DESIGN/06-scheduler.md § both forms, and the local
// one). Same log, same auction (one bidder), same guard as any fleet; no
// scheduler-shaped special case.
package fleet

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/executor"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/node"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/internal/workloads"
)

// LocalExecutorID is the embedded executor's durable identity — stable
// across restarts, per the workload contract.
const LocalExecutorID = "local"

// Config selects what the fleet runs.
type Config struct {
	// Dir is the data dir: bootstrap material, resolver, JetStream store,
	// per-tenant issuance material. Empty means devdir.Default().
	Dir string
	// Port for the bootstrap server; 0 means 4222, -1 picks a free port.
	Port int
	// Backend is the embedded executor's one configured actuator:
	// "inprocess" (the default, backend zero) or "microsandbox"
	// (06-scheduler.md § backends — the operator configures the backend
	// the install uses).
	Backend string
	// WorkloadBinary is the linux/arm64 chronicle-workload binary the
	// microsandbox backend copies into every guest; required with it,
	// ignored otherwise. `make workload-linux` builds it.
	WorkloadBinary string
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// The backend names the composition accepts.
const (
	BackendInProcess    = "inprocess"
	BackendMicrosandbox = "microsandbox"
)

// Fleet is one running composition.
type Fleet struct {
	URL string

	srv   *server.Server
	ctrl  micro.Service
	wl    *workloads.Service
	ex    *executor.Executor
	conns []*nats.Conn

	mu        sync.Mutex
	reporters map[string]*metaIndexReporter
}

// Up boots the fleet: bootstrap material generated or loaded, the embedded
// operator-mode server, the workload service, the embedded executor, and
// control — which dispatches a node workload for every tenant on disk, so
// placement flows the one path there is.
func Up(ctx context.Context, cfg Config) (*Fleet, error) {
	dir := cfg.Dir
	if dir == "" {
		dir = devdir.Default()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	port := cfg.Port
	if port == 0 {
		port = 4222
	}

	b, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		return nil, err
	}
	srv, err := b.StartServer(port)
	if err != nil {
		return nil, err
	}
	f := &Fleet{URL: srv.ClientURL(), srv: srv}
	if err := b.WriteClientURL(f.URL); err != nil {
		f.Stop()
		return nil, err
	}

	connect := func(creds []byte, name string) (*nats.Conn, error) {
		nc, err := mint.ConnectCreds(f.URL, creds, name)
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", name, err)
		}
		f.conns = append(f.conns, nc)
		return nc, nil
	}

	sysConn, err := connect(b.SysCreds, "chronicle-sys")
	if err != nil {
		f.Stop()
		return nil, err
	}
	ctrlConn, err := connect(b.ControlCreds, "chronicle-control")
	if err != nil {
		f.Stop()
		return nil, err
	}
	// In the embedded composition every control-plane component shares the
	// bootstrap control user on its own connection; per-component users
	// are custody the multi-host increment makes real.
	wlConn, err := connect(b.ControlCreds, "chronicle-workloads")
	if err != nil {
		f.Stop()
		return nil, err
	}
	exConn, err := connect(b.ControlCreds, "chronicle-executor-"+LocalExecutorID)
	if err != nil {
		f.Stop()
		return nil, err
	}

	wl, err := workloads.Start(ctx, wlConn, workloads.Config{Logger: logger})
	if err != nil {
		f.Stop()
		return nil, fmt.Errorf("start workload service: %w", err)
	}
	f.wl = wl

	pull := func(ctx context.Context, tenant, workload string) ([]byte, error) {
		return executor.PullCreds(ctx, exConn, LocalExecutorID, tenant, workload)
	}
	var backend executor.Backend
	switch cfg.Backend {
	case "", BackendInProcess:
		backend = &executor.InProcess{
			URL:   f.URL,
			Creds: pull,
			NodeConfig: func(tenant string) node.Config {
				return node.Config{Logger: logger, Indexes: &indexReporter{nc: ctrlConn, tenant: tenant, logger: logger}}
			},
			Logger: logger,
		}
	case BackendMicrosandbox:
		if cfg.WorkloadBinary == "" {
			f.Stop()
			return nil, fmt.Errorf("the microsandbox backend needs --workload-binary (make workload-linux builds it)")
		}
		backend = &executor.Microsandbox{
			WorkloadBinary: cfg.WorkloadBinary,
			HostURL:        f.URL,
			Creds:          pull,
			Logger:         logger,
		}
	default:
		f.Stop()
		return nil, fmt.Errorf("backend %q is not in this build's vocabulary (inprocess, microsandbox)", cfg.Backend)
	}
	ex, err := executor.Start(ctx, exConn, executor.Config{ID: LocalExecutorID, Backend: backend, Logger: logger})
	if err != nil {
		f.Stop()
		return nil, fmt.Errorf("start executor: %w", err)
	}
	f.ex = ex

	driver := &mint.JWTDriver{
		OperatorSigningSeed: b.OperatorSigningSeed,
		SysConn:             sysConn,
		URL:                 f.URL,
	}
	ctrl, err := control.Start(ctrlConn, control.Config{
		Driver:      driver,
		URL:         f.URL,
		AccountsDir: b.AccountsDir(),
		OnTenant: func(name string, serviceCreds []byte) error {
			// The tenant's node exists because the record says so; the
			// executor pulls the creds itself — the record-verified pull.
			dispatchCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
			defer cancel()
			if _, err := workloads.Dispatch(dispatchCtx, ctrlConn, contract.FleetDispatchRequest{
				Tenant:   name,
				Workload: contract.WorkloadNodeName,
				Kind:     contract.WorkloadKindNode,
			}); err != nil {
				return err
			}
			// An out-of-process node cannot reach the dispatch surface
			// without the multi-host bridge; the composition carries its
			// reports from META until the bridge lands.
			if cfg.Backend == BackendMicrosandbox {
				if err := f.startReporter(name, serviceCreds, ctrlConn, logger); err != nil {
					return err
				}
			}
			// Placement is asynchronous, but the mint's promise is not: a
			// minted tenant answers verbs (onboarding § verify by
			// connecting). Wait until the placed node serves.
			return waitForNode(dispatchCtx, f.URL, name, serviceCreds)
		},
		Logger: logger,
	})
	if err != nil {
		f.Stop()
		return nil, err
	}
	f.ctrl = ctrl
	return f, nil
}

// waitForNode probes the tenant's node over the micro protocol as its
// service user until the placement answers — the design's off-log
// liveness probe doubling as the could-not-succeed-if-broken read the
// mint promises.
func waitForNode(ctx context.Context, url, tenant string, serviceCreds []byte) error {
	nc, err := mint.ConnectCreds(url, serviceCreds, "chronicle-mint-verify-"+tenant)
	if err != nil {
		return fmt.Errorf("connect for mint verification: %w", err)
	}
	defer nc.Close()
	for {
		reqCtx, cancel := context.WithTimeout(ctx, time.Second)
		_, err := nc.RequestWithContext(reqCtx, "$SRV.PING.chronicle-node", nil)
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("tenant %s: node never answered: %w", tenant, ctx.Err())
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// indexReporter is the in-process transport of the node's dispatch
// reports; the tenant-stamped account import is the multi-host
// increment's transport for the same calls.
type indexReporter struct {
	nc     *nats.Conn
	tenant string
	logger *slog.Logger
}

func (r *indexReporter) IndexDeclared(ctx context.Context, log, index, kind string) error {
	if kind != contract.IndexKindSearch {
		// Read-side tolerance: a newer build may have declared kinds this
		// composition has no workload for.
		r.logger.Warn("declared index kind has no workload here; left alone", "tenant", r.tenant, "log", log, "index", index, "kind", kind)
		return nil
	}
	_, err := workloads.Dispatch(ctx, r.nc, contract.FleetDispatchRequest{
		Tenant:   r.tenant,
		Workload: contract.WorkloadIndexName(log, index),
		Kind:     contract.WorkloadKindIndexSearch,
		Log:      log,
		Index:    index,
	})
	return err
}

func (r *indexReporter) IndexDeleted(ctx context.Context, log, index string) error {
	_, err := workloads.StopWorkload(ctx, r.nc, contract.FleetStopRequest{
		Tenant:   r.tenant,
		Workload: contract.WorkloadIndexName(log, index),
	})
	return err
}

// startReporter runs one tenant's META-derived report transport,
// idempotently — the restart replay revisits every tenant.
func (f *Fleet) startReporter(tenant string, serviceCreds []byte, dispatchConn *nats.Conn, logger *slog.Logger) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reporters == nil {
		f.reporters = map[string]*metaIndexReporter{}
	}
	if _, running := f.reporters[tenant]; running {
		return nil
	}
	r, err := startMetaIndexReporter(f.URL, tenant, serviceCreds, dispatchConn, logger)
	if err != nil {
		return err
	}
	f.reporters[tenant] = r
	return nil
}

// Stop tears the composition down: control verbs first, then the workload
// service, then the executor and its placements, then the server. Appends
// need none of them — writers lose nothing.
func (f *Fleet) Stop() {
	if f.ctrl != nil {
		_ = f.ctrl.Stop()
	}
	f.mu.Lock()
	reporters := f.reporters
	f.reporters = nil
	f.mu.Unlock()
	for _, r := range reporters {
		r.stop()
	}
	if f.wl != nil {
		f.wl.Stop()
	}
	if f.ex != nil {
		f.ex.Stop()
	}
	conns := f.conns
	f.conns = nil
	for _, nc := range conns {
		nc.Close()
	}
	if f.srv != nil {
		f.srv.Shutdown()
	}
}

// Run is the `chronicle up` subcommand: parse flags, boot, print, block
// until ctx ends.
func Run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle up", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "data dir for the local fleet")
	port := fs.Int("port", 4222, "port for the bootstrap NATS server (-1 picks a free one)")
	backend := fs.String("backend", BackendInProcess, "the embedded executor's backend: inprocess or microsandbox")
	workloadBinary := fs.String("workload-binary", "", "linux/arm64 chronicle-workload for the microsandbox backend")
	if err := fs.Parse(args); err != nil {
		return err
	}

	f, err := Up(ctx, Config{Dir: *dir, Port: *port, Backend: *backend, WorkloadBinary: *workloadBinary})
	if err != nil {
		return err
	}
	defer f.Stop()

	fmt.Fprintf(out, "chronicle %s up\n", version.Version)
	fmt.Fprintf(out, "  url:  %s\n", f.URL)
	fmt.Fprintf(out, "  dir:  %s\n", *dir)
	fmt.Fprintf(out, "create a tenant:  chronicle tenant create <name> --dir %s\n", *dir)
	<-ctx.Done()
	return nil
}
