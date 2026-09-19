// Package natstest is a test-only helper that runs in-process NATS servers,
// so wire-contract tests need no external server. It is under internal/
// because it is not part of the module's public surface.
package natstest

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

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

// StartSealedOperator is design 10's first boot in a test: a root born in a
// temp dir, the embedded server encrypted at rest with the root's node key,
// the working keys sealed into the AUTH bucket over the first instance's
// bundle. Tests open custody and build drivers from what it returns.
func StartSealedOperator(t *testing.T) (url string, r *mint.Root) {
	t.Helper()
	r, err := mint.InitRoot(t.TempDir())
	if err != nil {
		t.Fatalf("init root: %v", err)
	}
	key, err := r.NodeKey("embedded")
	if err != nil {
		t.Fatalf("node key: %v", err)
	}
	srv, err := r.B.StartServerWithKey(-1, key)
	if err != nil {
		t.Fatalf("operator server: %v", err)
	}
	t.Cleanup(srv.Shutdown)
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	sysConn, err := mint.ConnectCreds(srv.ClientURL(), bundle.SysCreds, "seal-sys")
	if err != nil {
		t.Fatalf("connect for seal: %v", err)
	}
	defer sysConn.Close()
	nc, err := mint.ConnectCreds(srv.ClientURL(), bundle.ControlCreds, "seal")
	if err != nil {
		t.Fatalf("connect for seal: %v", err)
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := r.Seal(ctx, sysConn, nc, mint.SealOptions{Replicas: 1}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	return srv.ClientURL(), r
}

// OpenInstance connects a control instance's two users from the root's
// first bundle and opens custody — what chronicle-control does at boot.
func OpenInstance(t *testing.T, url string, r *mint.Root) (sysConn, ctrlConn *nats.Conn, c *mint.Custody) {
	t.Helper()
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	sysConn, err = mint.ConnectCreds(url, bundle.SysCreds, "test-instance-sys")
	if err != nil {
		t.Fatalf("connect system user: %v", err)
	}
	t.Cleanup(sysConn.Close)
	ctrlConn, err = mint.ConnectCreds(url, bundle.ControlCreds, "test-instance-control")
	if err != nil {
		t.Fatalf("connect control user: %v", err)
	}
	t.Cleanup(ctrlConn.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err = mint.OpenCustody(ctx, ctrlConn)
	if err != nil {
		t.Fatalf("open custody: %v", err)
	}
	return sysConn, ctrlConn, c
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
