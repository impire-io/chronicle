// Package fleet is the composition root: `chronicle up` — the embedded
// NATS, control, one workload-service instance, and one embedded executor
// on the in-process backend, the fleet shape without the fleet ceremony
// (chronicle-hq/02-DESIGN/06-scheduler.md § both forms) — and
// `chronicle-control`, one control instance standing over a substrate the
// operator runs (02-DESIGN/09-hosted-environment.md, 10-custody.md). One
// composition of control, both roots; no scheduler-shaped special case in
// either.
package fleet

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/executor"
	"github.com/impire-io/chronicle/internal/index/semantic"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/internal/workloads"
)

// LocalExecutorID is the embedded executor's durable identity — stable
// across restarts, per the workload contract.
const LocalExecutorID = "local"

// embeddedNode names the embedded server in the root's manifest: its
// JetStream key lives there, beside the keys of any emitted node.
const embeddedNode = "embedded"

// workloadsInstance names the workload service `up` runs — its bundle
// under the dev dir. The embedded executor's instance is LocalExecutorID:
// an executor's instance name is its ID (design 10 § the fence).
const workloadsInstance = "workloads"

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
	// WorkloadBinary is the linux chronicle-workload binary (the host's
	// architecture) the microsandbox backend copies into every guest;
	// required with it, ignored otherwise. `make workload-linux` builds it.
	WorkloadBinary string
	// Embedding is the install's provider (0016) — nil means no provider
	// and semantic declarations stay honestly unschedulable.
	Embedding *semantic.ProviderConfig
	// GithubClientID configures the browser identity bridge (decision
	// 0026) — the install's GitHub App. Empty means no bridge.
	GithubClientID string
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

	// Design 10's first boot, in one process: the root is born or opened,
	// the embedded server runs encrypted at rest under the root's node
	// key, the working keys are sealed into the AUTH bucket, and control
	// dials with the bundle the root issued — the same path a hosted
	// instance walks, with the ceremonies folded into `up`.
	r, err := mint.InitRoot(dir)
	if err != nil {
		return nil, err
	}
	nodeKey, err := r.NodeKey(embeddedNode)
	if err != nil {
		return nil, err
	}
	srv, err := r.B.StartServerWithKey(port, nodeKey)
	if err != nil {
		return nil, fmt.Errorf("%w (a dev dir from before design 10 holds an unencrypted store: move %s aside and start again)", err, dir)
	}
	f := &Fleet{URL: srv.ClientURL(), srv: srv}
	if err := r.B.WriteClientURL(f.URL); err != nil {
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

	bundle, err := mint.ReadBundle(r.BundleDir(firstInstance))
	if err != nil {
		f.Stop()
		return nil, err
	}
	sysConn, err := connect(bundle.SysCreds, "chronicle-sys")
	if err != nil {
		f.Stop()
		return nil, err
	}
	ctrlConn, err := connect(bundle.ControlCreds, "chronicle-control")
	if err != nil {
		f.Stop()
		return nil, err
	}
	if _, err := r.Seal(ctx, sysConn, ctrlConn, mint.SealOptions{Replicas: 1}); err != nil {
		f.Stop()
		return nil, fmt.Errorf("seal custody: %w", err)
	}
	custody, err := mint.OpenCustody(ctx, ctrlConn)
	if err != nil {
		f.Stop()
		return nil, err
	}
	driver, err := mint.NewJWTDriver(ctx, custody, sysConn, f.URL)
	if err != nil {
		f.Stop()
		return nil, err
	}
	// The fleet's members hold users of their own role (design 10 § the
	// fence): the workload service, the embedded executor, and the
	// operator's CLI, each an instance with a bundle under the dev dir —
	// issued over the bucket the first time, reused on every boot after.
	member := func(name string, t mint.Template) ([]byte, error) {
		if bundle, err := mint.ReadBundle(r.BundleDir(name)); err == nil {
			return bundle.ControlCreds, nil
		}
		bundle, err := driver.AddInstance(ctx, name, t)
		if err != nil {
			return nil, fmt.Errorf("issue %s user %s: %w", t, name, err)
		}
		if _, err := r.WriteBundle(name, bundle); err != nil {
			return nil, err
		}
		return bundle.ControlCreds, nil
	}
	wlCreds, err := member(workloadsInstance, mint.TemplateWorkloads)
	if err != nil {
		f.Stop()
		return nil, err
	}
	exCreds, err := member(LocalExecutorID, mint.TemplateExecutor)
	if err != nil {
		f.Stop()
		return nil, err
	}
	if _, err := member(devdir.CLIBundle, mint.TemplateCLI); err != nil {
		f.Stop()
		return nil, err
	}
	wlConn, err := connect(wlCreds, "chronicle-workloads")
	if err != nil {
		f.Stop()
		return nil, err
	}
	exConn, err := connect(exCreds, "chronicle-executor-"+LocalExecutorID)
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
			URL:       f.URL,
			Creds:     pull,
			Embedding: cfg.Embedding,
			Logger:    logger,
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
			Embedding:      cfg.Embedding,
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

	// Control last: its boot replay dispatches every tenant in custody,
	// and the workload service and the executor must be serving by then.
	ctrl, err := startControl(ctx, controlInputs{
		url:            f.URL,
		ctrlConn:       ctrlConn,
		custody:        custody,
		driver:         driver,
		githubClientID: cfg.GithubClientID,
		bridgeProfile:  filepath.Join(dir, bridgeProfileFile),
		logger:         logger,
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

// Stop tears the composition down: control verbs first, then the workload
// service, then the executor and its placements, then the server. Appends
// need none of them — writers lose nothing.
func (f *Fleet) Stop() {
	if f.ctrl != nil {
		_ = f.ctrl.Stop()
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
	workloadBinary := fs.String("workload-binary", "", "linux chronicle-workload (host arch) for the microsandbox backend")
	githubClientID := fs.String("github-client-id", "", "GitHub App client id — enables the browser identity bridge (0026)")
	embedURL := fs.String("embedding-url", "", "OpenAI-compatible embedding endpoint for the semantic kind (key via CHRONICLE_EMBEDDING_API_KEY)")
	embedModel := fs.String("embedding-model", "", "default embedding model for the semantic kind")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var embedding *semantic.ProviderConfig
	if *embedURL != "" && *embedModel != "" {
		embedding = &semantic.ProviderConfig{BaseURL: *embedURL, Model: *embedModel, APIKey: os.Getenv("CHRONICLE_EMBEDDING_API_KEY")}
	}

	f, err := Up(ctx, Config{Dir: *dir, Port: *port, Backend: *backend, WorkloadBinary: *workloadBinary, Embedding: embedding, GithubClientID: *githubClientID})
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

func writeBridgeProfile(path, url, clientID string, sentinel []byte) error {
	p := contract.BridgeProfile{URL: url, GithubClientID: clientID, Sentinel: string(sentinel)}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("encode bridge profile: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write bridge profile: %w", err)
	}
	return nil
}
