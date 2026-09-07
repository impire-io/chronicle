// Package fleet composes `chronicle up`: the bootstrap NATS, control, and
// a scheduler-less node per minted tenant, in one process — the fleet
// shape without the fleet ceremony (chronicle-hq/02-DESIGN/04-fleet.md
// § the walking skeleton floor).
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

	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/node"
	"github.com/impire-io/chronicle/internal/version"
)

// Config selects what the fleet runs.
type Config struct {
	// Dir is the data dir: bootstrap material, resolver, JetStream store,
	// per-tenant issuance material. Empty means devdir.Default().
	Dir string
	// Port for the bootstrap server; 0 means 4222, -1 picks a free port.
	Port int
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// Fleet is one running composition.
type Fleet struct {
	URL string

	srv   *server.Server
	ctrl  micro.Service
	conns []*nats.Conn

	mu    sync.Mutex
	nodes map[string]*node.Node
}

// Up boots the fleet: bootstrap material generated or loaded, the embedded
// operator-mode server, control, and a node for every tenant on disk.
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
	f := &Fleet{URL: srv.ClientURL(), srv: srv, nodes: map[string]*node.Node{}}
	if err := b.WriteClientURL(f.URL); err != nil {
		f.Stop()
		return nil, err
	}

	sysConn, err := mint.ConnectCreds(f.URL, b.SysCreds, "chronicle-sys")
	if err != nil {
		f.Stop()
		return nil, fmt.Errorf("connect system user: %w", err)
	}
	f.conns = append(f.conns, sysConn)
	ctrlConn, err := mint.ConnectCreds(f.URL, b.ControlCreds, "chronicle-control")
	if err != nil {
		f.Stop()
		return nil, fmt.Errorf("connect control user: %w", err)
	}
	f.conns = append(f.conns, ctrlConn)

	driver := &mint.JWTDriver{
		OperatorSigningSeed: b.OperatorSigningSeed,
		SysConn:             sysConn,
		URL:                 f.URL,
	}

	startNode := func(name string, serviceCreds []byte) error {
		nc, err := mint.ConnectCreds(f.URL, serviceCreds, "chronicle-node-"+name)
		if err != nil {
			return fmt.Errorf("connect node for %s: %w", name, err)
		}
		startCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		n, err := node.Start(startCtx, nc, node.Config{Logger: logger})
		if err != nil {
			nc.Close()
			return fmt.Errorf("start node for %s: %w", name, err)
		}
		f.mu.Lock()
		f.nodes[name] = n
		f.conns = append(f.conns, nc)
		f.mu.Unlock()
		logger.Info("node running", "tenant", name)
		return nil
	}

	ctrl, err := control.Start(ctrlConn, control.Config{
		Driver:      driver,
		URL:         f.URL,
		AccountsDir: b.AccountsDir(),
		OnTenant:    startNode,
		Logger:      logger,
	})
	if err != nil {
		f.Stop()
		return nil, err
	}
	f.ctrl = ctrl
	return f, nil
}

// Stop tears the composition down: control verbs first, then the nodes,
// then the server. Appends need none of them — writers lose nothing.
func (f *Fleet) Stop() {
	if f.ctrl != nil {
		_ = f.ctrl.Stop()
	}
	f.mu.Lock()
	for _, n := range f.nodes {
		n.Stop()
	}
	f.nodes = map[string]*node.Node{}
	conns := f.conns
	f.conns = nil
	f.mu.Unlock()
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
	if err := fs.Parse(args); err != nil {
		return err
	}

	f, err := Up(ctx, Config{Dir: *dir, Port: *port})
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
