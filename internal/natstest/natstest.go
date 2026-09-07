// Package natstest is a test-only helper that runs in-process NATS servers,
// so wire-contract tests need no external server. It is under internal/
// because it is not part of the module's public surface.
package natstest

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/impire-io/chronicle/internal/mint"
)

// StartJetStream runs an in-process NATS server with JetStream enabled,
// storing state in a per-test temp dir. The server is shut down when the
// test ends.
func StartJetStream(t *testing.T) (url string) {
	t.Helper()
	srv := startServer(t, &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	return srv.ClientURL()
}

// StartOperator runs an in-process operator-mode server with a freshly
// generated bootstrap in a per-test temp dir — the substrate the jwt
// driver mints against. The server is shut down when the test ends.
func StartOperator(t *testing.T) (url string, b *mint.Bootstrap) {
	t.Helper()
	b, err := mint.LoadOrInitBootstrap(t.TempDir())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	srv, err := b.StartServer(-1)
	if err != nil {
		t.Fatalf("operator server: %v", err)
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL(), b
}

func startServer(t *testing.T, opts *server.Options) *server.Server {
	t.Helper()
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready in time")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}
