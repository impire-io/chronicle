// Package up is the open quick start (chronicle-hq/02-DESIGN/11-the-two-
// forms.md § the quick start): an embedded nats-server with one account,
// the node, and its declared indexers in one process, keys under
// ~/.chronicle. No operator, no JWTs, no ceremony — five minutes to a
// folded state read. It is a development convenience and says so:
// production is the operator's own NATS, where the node and each indexer
// are the operator's own processes (chronicle-node, chronicle-workload).
// The composition that boots a whole fleet is the managed service's.
package up

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/devdir"
	"github.com/impire-io/chronicle/index/semantic"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/node"
	"github.com/impire-io/chronicle/registry"
)

// Config selects what the quick start runs.
type Config struct {
	// Dir is the data dir: the user's seed, the recorded url, the
	// JetStream store. Empty means devdir.Default().
	Dir string
	// Port for the embedded server; 0 means 4222, -1 picks a free port.
	Port int
	// WebsocketPort opens the browser's listener on loopback, in the clear
	// (09-hosted-environment.md § the websocket listener): 0 means none,
	// -1 picks a free port.
	WebsocketPort int
	// WebsocketOrigins are the origins a browser may connect from; none
	// means any — the listener is loopback-only and still authenticates.
	WebsocketOrigins []string
	// Embedding is the install's provider (0016) — nil means no provider
	// and semantic declarations stay honestly unserved.
	Embedding *semantic.ProviderConfig
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// Local is one running quick start.
type Local struct {
	// URL is the embedded server's client url.
	URL string
	// WebsocketURL is the browser's url, empty when no listener was asked for.
	WebsocketURL string

	srv   *server.Server
	node  *node.Node
	sup   *supervisor
	conns []*nats.Conn
}

// Up boots the quick start: the user's seed born or loaded, the embedded
// server with that one user on its one account, the registry seeded with
// the admin, the indexer supervisor listening for the node's reports, and
// the node — in that order, so a report has a listener from the node's
// first breath.
func Up(ctx context.Context, cfg Config) (*Local, error) {
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
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	kp, err := loadOrCreateUser(devdir.UserNkeyPath(dir))
	if err != nil {
		return nil, err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, fmt.Errorf("user public key: %w", err)
	}

	srv, err := startServer(serverConfig{
		port:      port,
		storeDir:  filepath.Join(dir, "jetstream"),
		userPub:   pub,
		wsPort:    cfg.WebsocketPort,
		wsOrigins: cfg.WebsocketOrigins,
	})
	if err != nil {
		return nil, err
	}
	l := &Local{URL: srv.ClientURL(), srv: srv}
	if cfg.WebsocketPort != 0 {
		l.WebsocketURL = srv.WebsocketURL()
	}
	if err := os.WriteFile(devdir.ClientURLPath(dir), []byte(l.URL), 0o600); err != nil {
		l.Stop()
		return nil, fmt.Errorf("record client url: %w", err)
	}
	if err := recordWebsocketURL(dir, l.WebsocketURL); err != nil {
		l.Stop()
		return nil, err
	}

	connect := func(name string) (*nats.Conn, error) {
		nc, err := nats.Connect(l.URL, nats.Name(name), nats.Nkey(pub, kp.Sign), nats.Timeout(5*time.Second))
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", name, err)
		}
		l.conns = append(l.conns, nc)
		return nc, nil
	}
	seedConn, err := connect("chronicle-up")
	if err != nil {
		l.Stop()
		return nil, err
	}
	js, err := jetstreamOf(seedConn)
	if err != nil {
		l.Stop()
		return nil, err
	}
	if _, err := registry.Seed(ctx, js, devdir.LocalPrincipal, ""); err != nil {
		l.Stop()
		return nil, err
	}

	supConn, err := connect("chronicle-up-indexers")
	if err != nil {
		l.Stop()
		return nil, err
	}
	sup, err := startSupervisor(supConn, cfg.Embedding, logger)
	if err != nil {
		l.Stop()
		return nil, err
	}
	l.sup = sup

	nodeConn, err := connect("chronicle-node")
	if err != nil {
		l.Stop()
		return nil, err
	}
	n, err := node.Start(ctx, nodeConn, node.Config{Logger: logger})
	if err != nil {
		l.Stop()
		return nil, fmt.Errorf("start node: %w", err)
	}
	l.node = n
	return l, nil
}

// Stop tears the quick start down: the node's verbs first, then the
// indexers, then the server. Appends need none of them.
func (l *Local) Stop() {
	if l.node != nil {
		l.node.Stop()
	}
	if l.sup != nil {
		l.sup.stop()
	}
	conns := l.conns
	l.conns = nil
	for _, nc := range conns {
		nc.Close()
	}
	if l.srv != nil {
		l.srv.Shutdown()
	}
}

// loadOrCreateUser reads the quick start's user seed, or births it: the
// one key the dir holds, mode 0600, stable across boots so the identity
// a context saved yesterday still speaks today.
func loadOrCreateUser(path string) (nkeys.KeyPair, error) {
	seed, err := os.ReadFile(path)
	if err == nil {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, fmt.Errorf("user seed %s: %w", path, err)
		}
		return kp, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read user seed: %w", err)
	}
	kp, err := nkeys.CreateUser()
	if err != nil {
		return nil, fmt.Errorf("create user key: %w", err)
	}
	seed, err = kp.Seed()
	if err != nil {
		return nil, fmt.Errorf("user seed: %w", err)
	}
	if err := os.WriteFile(path, seed, 0o600); err != nil {
		return nil, fmt.Errorf("write user seed: %w", err)
	}
	return kp, nil
}

// recordWebsocketURL writes the browser's url beside the client url, or
// removes a stale one: the file never names a socket that is not there.
func recordWebsocketURL(dir, url string) error {
	path := devdir.WebsocketURLPath(dir)
	if url == "" {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale websocket url: %w", err)
		}
		return nil
	}
	if err := os.WriteFile(path, []byte(url), 0o600); err != nil {
		return fmt.Errorf("record websocket url: %w", err)
	}
	return nil
}

// serverConfig is what the embedded server is started with.
type serverConfig struct {
	port      int
	storeDir  string
	userPub   string
	wsPort    int // 0: no websocket listener
	wsOrigins []string
}

// startServer runs the embedded server: one account — the server's own
// global account, JetStream on — with one nkey user, and nothing else
// may connect. Any NATS in any auth mode is the production shape; this
// is the smallest one that has an account and a user at all. The
// websocket listener, when asked for, binds loopback in the clear — the
// one place design 09 allows it — and authenticates the same one user.
func startServer(sc serverConfig) (*server.Server, error) {
	opts := &server.Options{
		ServerName: "chronicle-local",
		Host:       "127.0.0.1",
		Port:       sc.port,
		JetStream:  true,
		StoreDir:   sc.storeDir,
		Nkeys:      []*server.NkeyUser{{Nkey: sc.userPub}},
		NoSigs:     true,
	}
	if sc.wsPort != 0 {
		opts.Websocket = server.WebsocketOpts{
			Host:           "127.0.0.1",
			Port:           sc.wsPort,
			NoTLS:          true,
			AllowedOrigins: sc.wsOrigins,
		}
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("new nats server: %w", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		return nil, fmt.Errorf("nats server not ready in time")
	}
	return srv, nil
}

// Run is the `chronicle up` verb: parse flags, boot, print, block until
// ctx ends.
func Run(ctx context.Context, args []string, out io.Writer) error {
	return RunWith(ctx, args, out, nil)
}

// RunWith is Run with a hook the binary wires: once the quick start is
// up, ready is called with the running local and its data dir — the CLI
// saves and selects its `local` context there (decision 0045 § 5). This
// package never imports the adapters; the binary composes the two.
func RunWith(ctx context.Context, args []string, out io.Writer, ready func(l *Local, dir string) error) error {
	fs := flag.NewFlagSet("chronicle up", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() {
		fmt.Fprintln(out, "Usage: chronicle up [--dir DIR] [--port N] [--websocket-port N] [--websocket-origin URL]... [--embedding-url URL --embedding-model NAME]")
		fmt.Fprintln(out, "  run chronicle locally: an embedded NATS server, one account, one user, the node and its indexes, in one process")
		fmt.Fprintln(out, "  a development convenience, not production — production is your own NATS, with chronicle-node and chronicle-workload as your own processes")
		fmt.Fprintln(out, "\nFlags:")
		fs.PrintDefaults()
	}
	for _, a := range args {
		if a == "-h" || a == "--help" || a == "-help" {
			fs.Usage()
			return nil
		}
	}
	dir := fs.String("dir", devdir.Default(), "data dir: the user's seed, the recorded url, the store")
	port := fs.Int("port", 4222, "port for the embedded NATS server (-1 picks a free one)")
	wsPort := fs.Int("websocket-port", 0, "open a websocket listener for a browser on loopback, in the clear (-1 picks a free port; 0 means none)")
	var wsOrigins []string
	fs.Func("websocket-origin", "an origin a browser may connect from, e.g. http://localhost:3000 (repeatable; none means any)", func(v string) error {
		wsOrigins = append(wsOrigins, v)
		return nil
	})
	embedURL := fs.String("embedding-url", "", "OpenAI-compatible embedding endpoint for the semantic kind (key via CHRONICLE_EMBEDDING_API_KEY)")
	embedModel := fs.String("embedding-model", "", "default embedding model for the semantic kind")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var embedding *semantic.ProviderConfig
	if *embedURL != "" && *embedModel != "" {
		embedding = &semantic.ProviderConfig{BaseURL: *embedURL, Model: *embedModel, APIKey: os.Getenv("CHRONICLE_EMBEDDING_API_KEY")}
	}

	l, err := Up(ctx, Config{Dir: *dir, Port: *port, WebsocketPort: *wsPort, WebsocketOrigins: wsOrigins, Embedding: embedding})
	if err != nil {
		return err
	}
	defer l.Stop()

	fmt.Fprintf(out, "chronicle %s is up: one account, one user (%s), the node and its indexes\n", version.Version, devdir.LocalPrincipal)
	fmt.Fprintf(out, "  url:  %s\n", l.URL)
	if l.WebsocketURL != "" {
		fmt.Fprintf(out, "  ws:   %s\n", l.WebsocketURL)
	}
	fmt.Fprintf(out, "  dir:  %s\n", *dir)
	if ready != nil {
		if err := ready(l, *dir); err != nil {
			fmt.Fprintf(out, "  (the local context was not saved: %v)\n", err)
		} else {
			fmt.Fprintf(out, "  context: local (saved; selected unless another context already was)\n")
		}
	}
	fmt.Fprintf(out, "next: chronicle store create NAME")
	if *dir != devdir.Default() {
		fmt.Fprintf(out, " --dir %s", *dir)
	}
	fmt.Fprintln(out)
	<-ctx.Done()
	return nil
}
