// Package natstest is a test-only helper that runs an in-process NATS
// server, so wire-contract tests need no external server. It is under
// internal/ because it is not part of the module's public surface.
package natstest

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// StartJetStream runs an in-process NATS server with JetStream enabled,
// storing state in a per-test temp dir. The server is shut down when the
// test ends. One account, no auth: the in-account wire behavior the
// tenant plane is tested for is account-independent.
func StartJetStream(t *testing.T) (url string) {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready in time")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}
