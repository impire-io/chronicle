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

	srv, err := startServer(port, filepath.Join(dir, "jetstream"), pub)
	if err != nil {
		return nil, err
	}
	l := &Local{URL: srv.ClientURL(), srv: srv}
	if err := os.WriteFile(devdir.ClientURLPath(dir), []byte(l.URL), 0o600); err != nil {
		l.Stop()
		return nil, fmt.Errorf("record client url: %w", err)
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
	if _, err := registry.Seed(ctx, js, devdir.LocalPrincipal); err != nil {
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

// startServer runs the embedded server: one account — the server's own
// global account, JetStream on — with one nkey user, and nothing else
// may connect. Any NATS in any auth mode is the production shape; this
// is the smallest one that has an account and a user at all.
func startServer(port int, storeDir, userPub string) (*server.Server, error) {
	opts := &server.Options{
		ServerName: "chronicle-local",
		Host:       "127.0.0.1",
		Port:       port,
		JetStream:  true,
		StoreDir:   storeDir,
		Nkeys:      []*server.NkeyUser{{Nkey: userPub}},
		NoSigs:     true,
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
	fs := flag.NewFlagSet("chronicle up", flag.ContinueOnError)
	fs.SetOutput(out)
	dir := fs.String("dir", devdir.Default(), "data dir: the user's seed, the recorded url, the store")
	port := fs.Int("port", 4222, "port for the embedded NATS server (-1 picks a free one)")
	embedURL := fs.String("embedding-url", "", "OpenAI-compatible embedding endpoint for the semantic kind (key via CHRONICLE_EMBEDDING_API_KEY)")
	embedModel := fs.String("embedding-model", "", "default embedding model for the semantic kind")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var embedding *semantic.ProviderConfig
	if *embedURL != "" && *embedModel != "" {
		embedding = &semantic.ProviderConfig{BaseURL: *embedURL, Model: *embedModel, APIKey: os.Getenv("CHRONICLE_EMBEDDING_API_KEY")}
	}

	l, err := Up(ctx, Config{Dir: *dir, Port: *port, Embedding: embedding})
	if err != nil {
		return err
	}
	defer l.Stop()

	fmt.Fprintf(out, "chronicle %s up — one account, one user (%s), the node, its indexers\n", version.Version, devdir.LocalPrincipal)
	fmt.Fprintf(out, "  url:  %s\n", l.URL)
	fmt.Fprintf(out, "  dir:  %s\n", *dir)
	fmt.Fprintf(out, "create a log:  chronicle log create <log>")
	if *dir != devdir.Default() {
		fmt.Fprintf(out, " --dir %s", *dir)
	}
	fmt.Fprintln(out)
	<-ctx.Done()
	return nil
}
