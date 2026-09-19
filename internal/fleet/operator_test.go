package fleet_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/fleet"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/natstest"
)

// TestInstanceAddAndRemove is design 10 § instances on a real server: the
// ceremony issues a second control instance over the bucket, and that
// instance — from its bundle and a URL alone — serves and mints; a fleet
// instance reaches the verbs and never the bucket; removing an instance
// evicts it and its bundle is dead wherever it went; a name comes back;
// and the root refuses to remove the last instance it could run the
// ceremony as.
func TestInstanceAddAndRemove(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	url, r := natstest.StartSealedOperator(t)
	_, _, custody1 := natstest.OpenInstance(t, url, r)

	run := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := fleet.Instance(ctx, append(args, "--dir", r.Dir, "--url", url), &out)
		return out.String(), err
	}

	// A second control instance, from the ceremony.
	out, err := run("add", "instance-2")
	if err != nil {
		t.Fatalf("add instance-2: %v\n%s", err, out)
	}
	if !strings.Contains(out, "chronicle-control --url") {
		t.Fatalf("add told the operator nothing about starting the instance:\n%s", out)
	}
	bundleDir := r.BundleDir("instance-2")
	info, err := os.Stat(bundleDir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("bundle dir %s: %v, mode %v", bundleDir, err, info.Mode())
	}
	b2, err := mint.ReadBundle(bundleDir)
	if err != nil || len(b2.SysCreds) == 0 || b2.Template() != mint.TemplateControlInstance {
		t.Fatalf("instance-2's bundle: %+v, %v", b2, err)
	}
	if _, err := run("add", "instance-2"); !errors.Is(err, mint.ErrInstanceExists) {
		t.Fatalf("a second add of the same name = %v, want ErrInstanceExists", err)
	}

	// The second plane: bundle + URL, nothing else; it serves and mints,
	// and the first instance sees the tenant in the bucket they share.
	sys2, err := mint.ConnectCreds(url, b2.SysCreds, "instance-2-sys")
	if err != nil {
		t.Fatalf("instance-2 sys: %v", err)
	}
	defer sys2.Close()
	ctrl2, err := mint.ConnectCreds(url, b2.ControlCreds, "instance-2")
	if err != nil {
		t.Fatalf("instance-2 control: %v", err)
	}
	defer ctrl2.Close()
	custody2, err := mint.OpenCustody(ctx, ctrl2)
	if err != nil {
		t.Fatalf("instance-2 opens custody: %v", err)
	}
	driver2, err := mint.NewJWTDriver(ctx, custody2, sys2, url)
	if err != nil {
		t.Fatalf("instance-2 driver: %v", err)
	}
	svc2, err := control.Start(ctrl2, control.Config{Driver: driver2, URL: url})
	if err != nil {
		t.Fatalf("instance-2 control: %v", err)
	}
	defer func() { _ = svc2.Stop() }()
	cc, err := client.ConnectControlCreds(url, b2.ControlCreds)
	if err != nil {
		t.Fatalf("connect control client: %v", err)
	}
	minted, err := cc.MintTenant(ctx, "acme", "dana")
	cc.Close()
	if err != nil {
		t.Fatalf("mint through instance-2: %v", err)
	}
	if _, _, err := custody1.Tenant(ctx, "acme"); err != nil {
		t.Fatalf("instance-1 does not see the tenant instance-2 minted: %v", err)
	}
	dana, err := mint.ConnectCreds(url, minted.AdminCreds, "dana")
	if err != nil {
		t.Fatalf("the minted admin cannot connect: %v", err)
	}
	dana.Close()

	// A fleet instance: one file, every verb, never the bucket.
	if out, err := run("add", "worker-1", "--template", "fleet"); err != nil {
		t.Fatalf("add worker-1: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(r.BundleDir("worker-1"), "sys.creds")); !os.IsNotExist(err) {
		t.Fatalf("a fleet bundle holds a SYS user (%v)", err)
	}
	w, err := mint.ReadBundle(r.BundleDir("worker-1"))
	if err != nil || w.Template() != mint.TemplateFleet {
		t.Fatalf("worker-1's bundle: %+v, %v", w, err)
	}
	assertFenced(t, url, w.ControlCreds, "worker-1")
	wc, err := client.ConnectControlCreds(url, w.ControlCreds)
	if err != nil {
		t.Fatalf("fleet user connects: %v", err)
	}
	if _, err := wc.MintTenant(ctx, "beta", ""); err != nil {
		wc.Close()
		t.Fatalf("the fleet user cannot reach control's verbs: %v", err)
	}
	wc.Close()

	// Removal is revocation: the live instance is evicted, the bundle is
	// refused, the name is free again.
	if out, err := run("remove", "instance-2"); err != nil {
		t.Fatalf("remove instance-2: %v\n%s", err, out)
	}
	waitDisconnected(t, ctrl2, "instance-2 control")
	waitDisconnected(t, sys2, "instance-2 sys")
	if nc, err := mint.ConnectCreds(url, b2.ControlCreds, "instance-2-again"); err == nil {
		nc.Close()
		t.Fatal("a removed instance's bundle still connects")
	}
	if _, err := os.Stat(bundleDir); !os.IsNotExist(err) {
		t.Fatalf("the root kept the removed bundle (%v)", err)
	}
	if _, err := run("remove", "instance-2"); !errors.Is(err, mint.ErrNoSuchInstance) {
		t.Fatalf("a second remove = %v, want ErrNoSuchInstance", err)
	}
	if out, err := run("add", "instance-2"); err != nil {
		t.Fatalf("re-add instance-2: %v\n%s", err, out)
	}
	b2b, err := mint.ReadBundle(bundleDir)
	if err != nil || bytes.Equal(b2b.ControlCreds, b2.ControlCreds) {
		t.Fatalf("re-added bundle: %v (same creds as before: %v)", err, bytes.Equal(b2b.ControlCreds, b2.ControlCreds))
	}
	nc, err := mint.ConnectCreds(url, b2b.ControlCreds, "instance-2-reissued")
	if err != nil {
		t.Fatalf("re-added instance cannot connect: %v", err)
	}
	nc.Close()
	if out, err := run("remove", "worker-1"); err != nil {
		t.Fatalf("remove worker-1: %v\n%s", err, out)
	}
	if nc, err := mint.ConnectCreds(url, w.ControlCreds, "worker-1-again"); err == nil {
		nc.Close()
		t.Fatal("a removed fleet user still connects")
	}

	// The ceremony never removes the instance it runs as: instance-1 goes
	// through instance-2's bundle, and then instance-2 is the last.
	if out, err := run("remove", "instance-1"); err != nil {
		t.Fatalf("remove instance-1 via instance-2: %v\n%s", err, out)
	}
	if _, err := run("remove", "instance-2"); err == nil || !strings.Contains(err.Error(), "add another instance first") {
		t.Fatalf("removing the last control instance = %v, want a refusal naming the fix", err)
	}
}

// assertFenced is the fence on one credential: the bucket cannot be
// opened, read directly, or written. Every refusal is a dropped request,
// so short deadlines keep them cheap.
func assertFenced(t *testing.T, url string, creds []byte, who string) {
	t.Helper()
	nc, err := mint.ConnectCreds(url, creds, who+"-fence")
	if err != nil {
		t.Fatalf("%s: connect: %v", who, err)
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := mint.OpenCustody(ctx, nc); err == nil {
		t.Fatalf("%s opened the AUTH bucket", who)
	}
	if _, err := nc.Request("$JS.API.DIRECT.GET.KV_AUTH.$KV.AUTH.operator", nil, 2*time.Second); err == nil {
		t.Fatalf("%s read the operator entry", who)
	}
	if _, err := nc.Request("$KV.AUTH.operator", []byte(`{"forged":true}`), 2*time.Second); err == nil {
		t.Fatalf("%s wrote the operator entry", who)
	}
}

// waitDisconnected polls until the server has closed the connection —
// revocation evicts actively, and the client parks in reconnect.
func waitDisconnected(t *testing.T, nc *nats.Conn, who string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for nc.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatalf("%s still connected: eviction did not land", who)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
