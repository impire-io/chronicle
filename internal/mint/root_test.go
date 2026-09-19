package mint_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/internal/mint"
)

// TestInitRootSealsAndExports is design 10's first boot on a real server:
// init births the root and the first instance's bundle; the bundle alone
// connects; seal moves the working keys into the bucket, reads them back,
// and exports; a second seal matches everything and writes nothing; a
// different root against the same bucket is refused.
func TestInitRootSealsAndExports(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()

	r, err := mint.InitRoot(dir)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	for _, f := range []string{"root.json", "sys-account.nk", "control-account.nk",
		filepath.Join("bundles", "instance-1", "control.creds"), filepath.Join("bundles", "instance-1", "sys.creds")} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatalf("init left no %s: %v", f, err)
		}
	}
	ctrl1, _ := os.ReadFile(filepath.Join(dir, "bundles", "instance-1", "control.creds"))
	again, err := mint.InitRoot(dir)
	if err != nil {
		t.Fatalf("second init: %v", err)
	}
	ctrl2, _ := os.ReadFile(filepath.Join(dir, "bundles", "instance-1", "control.creds"))
	if again.B.OperatorJWT != r.B.OperatorJWT || string(ctrl1) != string(ctrl2) {
		t.Fatal("a second init regenerated material instead of opening the root")
	}

	// Node keys: born once, stable, persisted.
	k1, err := r.NodeKey("nats-1")
	if err != nil || len(k1) != 64 {
		t.Fatalf("node key = %q, %v", k1, err)
	}
	if k1b, _ := r.NodeKey("nats-1"); k1b != k1 {
		t.Fatal("node key changed between calls")
	}
	loaded, err := mint.LoadRoot(dir)
	if err != nil || loaded.Manifest.Nodes["nats-1"].JetStreamKey != k1 {
		t.Fatalf("node key not persisted: %+v, %v", loaded.Manifest.Nodes, err)
	}

	// The substrate serves; the bundle is all an instance needs to connect.
	srv, err := r.B.StartServer(-1)
	if err != nil {
		t.Fatalf("substrate: %v", err)
	}
	defer srv.Shutdown()
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		t.Fatalf("read bundle: %v", err)
	}
	nc, err := mint.ConnectCreds(srv.ClientURL(), bundle.ControlCreds, "instance-1")
	if err != nil {
		t.Fatalf("connect with the bundle's control user: %v", err)
	}
	defer nc.Close()
	sysnc, err := mint.ConnectCreds(srv.ClientURL(), bundle.SysCreds, "instance-1-sys")
	if err != nil {
		t.Fatalf("connect with the bundle's system user: %v", err)
	}
	sysnc.Close()

	rep, err := r.Seal(ctx, nc, mint.SealOptions{Replicas: 1})
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if len(rep.Written) != 5 || len(rep.Matched) != 0 || rep.Export == "" {
		t.Fatalf("first seal: %+v", rep)
	}
	if _, err := os.Stat(rep.Export); err != nil {
		t.Fatalf("export missing: %v", err)
	}
	c, err := mint.OpenCustody(ctx, nc)
	if err != nil {
		t.Fatalf("open after seal: %v", err)
	}
	op, _, err := c.Operator(ctx)
	if err != nil || op.SigningSeed != string(r.B.OperatorSigningSeed) {
		t.Fatalf("operator in the bucket = %+v, %v", op, err)
	}
	ctrlAcct, _, err := c.Account(ctx, "CONTROL")
	if err != nil || ctrlAcct.JWT != r.B.ControlAccountJWT || ctrlAcct.Seed != string(r.B.ControlAccountSeed) {
		t.Fatalf("CONTROL in the bucket = %+v, %v", ctrlAcct, err)
	}
	auth, _, err := c.Auth(ctx)
	if err != nil || auth.XKeySeed != string(r.B.AuthXKeySeed) || !strings.Contains(auth.SentinelCreds, "NATS USER JWT") {
		t.Fatalf("auth in the bucket = %+v, %v", auth, err)
	}

	rep2, err := r.Seal(ctx, nc, mint.SealOptions{Replicas: 1})
	if err != nil {
		t.Fatalf("second seal: %v", err)
	}
	if len(rep2.Written) != 0 || len(rep2.Matched) != 5 {
		t.Fatalf("second seal: %+v", rep2)
	}
	reloaded, err := mint.LoadRoot(dir)
	if err != nil || reloaded.Manifest.Sealed == "" || len(reloaded.Manifest.Exports) != 2 {
		t.Fatalf("manifest after two seals: %+v, %v", reloaded.Manifest, err)
	}

	// Another root cannot seal over this one.
	other, err := mint.InitRoot(t.TempDir())
	if err != nil {
		t.Fatalf("other root: %v", err)
	}
	if _, err := other.Seal(ctx, nc, mint.SealOptions{Replicas: 1}); err == nil || !strings.Contains(err.Error(), "seal refused") {
		t.Fatalf("a different root sealed over the bucket: %v", err)
	}
	if op2, _, _ := c.Operator(ctx); op2.SigningSeed != string(r.B.OperatorSigningSeed) {
		t.Fatal("the refused seal changed the bucket")
	}
}
