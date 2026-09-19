// Package fleet composes `chronicle up`: the bootstrap NATS, control, one
// chronicle-workloads instance, and one embedded executor on the
// in-process backend — the fleet shape without the fleet ceremony
// (chronicle-hq/02-DESIGN/06-scheduler.md § both forms, and the local
// one). Same log, same auction (one bidder), same guard as any fleet; no
// scheduler-shaped special case.
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
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/executor"
	"github.com/impire-io/chronicle/internal/identity/github"
	"github.com/impire-io/chronicle/internal/index/semantic"
	"github.com/impire-io/chronicle/internal/mint"
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

	var bridgeCfg *control.BridgeConfig
	if cfg.GithubClientID != "" {
		authConn, err := connect(b.BridgeCreds, "chronicle-bridge")
		if err != nil {
			f.Stop()
			return nil, err
		}
		bridgeCfg = &control.BridgeConfig{
			Conn:               authConn,
			ResponseSignerSeed: b.AuthAccountSeed,
			XKeySeed:           b.AuthXKeySeed,
			Validator:          &github.Client{ClientID: cfg.GithubClientID},
			Logger:             logger,
		}
		// The bridge profile is the hand-out that makes `chronicle login`
		// possible — public material only (0026: the sentinel is public
		// by design).
		if err := writeBridgeProfile(cfg.Dir, f.URL, cfg.GithubClientID, b.SentinelCreds); err != nil {
			f.Stop()
			return nil, err
		}
	}

	driver := b.Driver(sysConn, f.URL)
	ctrl, err := control.Start(ctrlConn, control.Config{
		Driver:      driver,
		URL:         f.URL,
		AccountsDir: b.AccountsDir(),
		Bridge:      bridgeCfg,
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

// RotateSigningKey is `chronicle operator rotate-signing-key`: the offline
// trust-root ceremony, dispatched from cmd/chronicle like `up`. It is a
// custody operation on the fleet dir, not a wire verb — the adapters hold
// no minting material, so it lives with the composition root.
func RotateSigningKey(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operator rotate-signing-key", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "data dir for the local fleet")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("operator rotate-signing-key takes no positionals")
	}
	newPub, err := mint.RotateOperatorSigningKey(*dir)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "operator signing key rotated: %s\n", newPub)
	fmt.Fprintln(out, "every account re-signed and verified; start the fleet to serve under the new key")
	return nil
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

// nodeSpecs collects repeatable --node flags: <name>=<host>[:client[:cluster]].
type nodeSpecs []mint.ClusterNode

func (n *nodeSpecs) String() string { return fmt.Sprintf("%d nodes", len(*n)) }

func (n *nodeSpecs) Set(v string) error {
	name, rest, ok := strings.Cut(v, "=")
	if !ok || name == "" || rest == "" {
		return fmt.Errorf("--node wants <name>=<host>[:client-port[:cluster-port]], got %q", v)
	}
	node := mint.ClusterNode{Name: name}
	parts := strings.Split(rest, ":")
	node.Host = parts[0]
	if len(parts) > 3 {
		return fmt.Errorf("--node %q: too many port fields", v)
	}
	var err error
	if len(parts) > 1 {
		if node.ClientPort, err = strconv.Atoi(parts[1]); err != nil {
			return fmt.Errorf("--node %q: client port: %w", v, err)
		}
	}
	if len(parts) > 2 {
		if node.ClusterPort, err = strconv.Atoi(parts[2]); err != nil {
			return fmt.Errorf("--node %q: cluster port: %w", v, err)
		}
	}
	*n = append(*n, node)
	return nil
}

// EmitClusterConfig is `chronicle operator emit-cluster-config`: the
// stand-up ceremony's rendering step (chronicle-hq/02-DESIGN/09-hosted-
// environment.md), dispatched from cmd/chronicle like the other custody
// operations on the fleet dir. Pure rendering — the only writes are the
// per-node .conf files under --out.
func EmitClusterConfig(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("chronicle operator emit-cluster-config", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "data dir holding the bootstrap material")
	outDir := fs.String("out", ".", "directory the per-node configs are written to")
	var nodes nodeSpecs
	fs.Var(&nodes, "node", "<name>=<host>[:client-port[:cluster-port]] — repeat per node")
	clientPort := fs.Int("client-port", 0, "client port for every node (default 4222)")
	clusterPort := fs.Int("cluster-port", 0, "cluster port for every node (default 6222)")
	clusterName := fs.String("cluster-name", "", "cluster name (default CHRONICLE)")
	tlsCert := fs.String("tls-cert", "", "target-host path of the client-listener TLS cert")
	tlsKey := fs.String("tls-key", "", "target-host path of the client-listener TLS key")
	storeDir := fs.String("store-dir", "", "target-host JetStream dir (default /var/lib/chronicle/jetstream)")
	resolverDir := fs.String("resolver-dir", "", "target-host resolver dir (default /var/lib/chronicle/resolver)")
	listenHost := fs.String("listen-host", "", "bind address for both listeners (default 0.0.0.0)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("operator emit-cluster-config takes no positionals")
	}

	r, err := mint.InitRoot(*dir)
	if err != nil {
		return err
	}
	// Every emitted node encrypts its store at rest with its own key, born
	// here the first time the node is named and kept in the root only.
	for _, n := range nodes {
		if _, err := r.NodeKey(n.Name); err != nil {
			return err
		}
	}
	cfgs, err := r.B.EmitClusterConfigs(mint.ClusterConfig{
		Nodes:       nodes,
		ClientPort:  *clientPort,
		ClusterPort: *clusterPort,
		ClusterName: *clusterName,
		TLSCert:     *tlsCert,
		TLSKey:      *tlsKey,
		StoreDir:    *storeDir,
		ResolverDir: *resolverDir,
		ListenHost:  *listenHost,
	})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}
	for _, c := range cfgs {
		path := filepath.Join(*outDir, c.Name+".conf")
		if err := os.WriteFile(path, []byte(c.Content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
		fmt.Fprintf(out, "wrote %s\n", path)
	}
	fmt.Fprintln(out, "copy each config to its host and run: nats-server -c <name>.conf")
	fmt.Fprintf(out, "each node's %s is recorded in %s — set it in the node's unit environment; the configs carry no seeds and no keys\n", mint.JetStreamKeyEnv, filepath.Join(r.Dir, "root.json"))
	return nil
}

func writeBridgeProfile(dir, url, clientID string, sentinel []byte) error {
	if dir == "" {
		dir = devdir.Default()
	}
	p := contract.BridgeProfile{URL: url, GithubClientID: clientID, Sentinel: string(sentinel)}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("encode bridge profile: %w", err)
	}
	path := filepath.Join(dir, "bridge.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write bridge profile: %w", err)
	}
	return nil
}
