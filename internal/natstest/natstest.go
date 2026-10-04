// Package natstest is a test-only helper that runs an in-process NATS
// server, so wire-contract tests need no external server. It is under
// internal/ because it is not part of the module's public surface.
package natstest

import (
	"fmt"
	"os"
	"path/filepath"
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

// StartJetStreamLimited is StartJetStream with the one account under
// JetStream limits — limits is the body of an account's jetstream block in
// server config (e.g. `max_file: 1G, disk_max_stream_bytes: 64M`). Clients
// connect without credentials and land in that account (no_auth_user), so
// a test speaks to a limited account the way it speaks to the plain one.
func StartJetStreamLimited(t *testing.T, limits string) (url string) {
	t.Helper()
	dir := t.TempDir()
	conf := filepath.Join(dir, "nats.conf")
	body := fmt.Sprintf(`listen: "127.0.0.1:-1"
jetstream { store_dir: %q }
accounts { APP { jetstream { %s }, users: [ { user: app, password: app } ] } }
no_auth_user: app
`, filepath.Join(dir, "js"), limits)
	if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
		t.Fatalf("write nats config: %v", err)
	}
	opts, err := server.ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("nats config: %v", err)
	}
	srv, err := server.NewServer(opts)
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
