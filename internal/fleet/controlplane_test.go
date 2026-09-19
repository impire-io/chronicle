package fleet_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/executor"
	"github.com/impire-io/chronicle/internal/fleet"
	"github.com/impire-io/chronicle/internal/mint"
)

// standaloneExecutor is chronicle-executor's shape in-process: its own
// control-account connection, the in-process backend, a durable id.
type standaloneExecutor struct {
	nc *nats.Conn
	ex *executor.Executor
}

func startStandaloneExecutor(ctx context.Context, url string, controlCreds []byte, id string) (*standaloneExecutor, error) {
	nc, err := mint.ConnectCreds(url, controlCreds, "chronicle-executor-"+id)
	if err != nil {
		return nil, err
	}
	pull := func(ctx context.Context, tenant, workload string) ([]byte, error) {
		return executor.PullCreds(ctx, nc, id, tenant, workload)
	}
	ex, err := executor.Start(ctx, nc, executor.Config{ID: id, Backend: &executor.InProcess{URL: url, Creds: pull}})
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &standaloneExecutor{nc: nc, ex: ex}, nil
}

func (s *standaloneExecutor) stop() {
	s.ex.Stop()
	s.nc.Close()
}

func waitForStateSeq(ctx context.Context, t *testing.T, c *client.Client, log, thing string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		sv, err := c.State(ctx, log, thing)
		if err == nil && sv.Seq >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state of %s never reached seq %d: %v", thing, want, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestStandaloneControlPlane is design 09's stand-up ceremony in miniature:
// the substrate serves first, the control plane stands over it with no
// executor anywhere, a standalone executor joins over the wire, and a mint
// ends the way onboarding demands — the tenant's node placed and answering.
// Then the hosted boot order: everything but the substrate restarts,
// control first and the executor a beat later, and the boot replay must
// converge through the level scan rather than fail the unit.
func TestStandaloneControlPlane(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	b, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	srv, err := b.StartServer(-1)
	if err != nil {
		t.Fatalf("substrate: %v", err)
	}
	defer srv.Shutdown()
	url := srv.ClientURL()

	cp, err := fleet.StartControlPlane(ctx, fleet.ControlPlaneConfig{Dir: dir, URL: url})
	if err != nil {
		t.Fatalf("control plane: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			cp.Stop()
		}
	}()

	ex, err := startStandaloneExecutor(ctx, url, b.ControlCreds, "host-1")
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	exStopped := false
	defer func() {
		if !exStopped {
			ex.stop()
		}
	}()

	// The mint probe: mint, connect, read.
	ctrl, err := client.ConnectControlCreds(url, b.ControlCreds)
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	minted, err := ctrl.MintTenant(ctx, "acme", "dana")
	ctrl.Close()
	if err != nil {
		t.Fatalf("mint tenant: %v", err)
	}
	dana, err := client.Connect(url, minted.AdminCreds)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	if _, err := dana.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	birth, err := dana.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"total":1}`))
	if err != nil {
		t.Fatalf("create thing: %v", err)
	}
	waitForStateSeq(ctx, t, dana, "orders", "invoice.invoice-1", birth.Seq)
	dana.Close()

	// The hosted boot order. The control plane's replay dispatches the
	// tenant on disk before any executor is registered; the executor
	// arrives a second later and the scan must hand it the slot.
	cp.Stop()
	stopped = true
	ex.stop()
	exStopped = true

	type lateStart struct {
		ex  *standaloneExecutor
		err error
	}
	late := make(chan lateStart, 1)
	go func() {
		time.Sleep(time.Second)
		ex, err := startStandaloneExecutor(ctx, url, b.ControlCreds, "host-1")
		late <- lateStart{ex, err}
	}()
	cp2, err := fleet.StartControlPlane(ctx, fleet.ControlPlaneConfig{Dir: dir, URL: url})
	if err != nil {
		t.Fatalf("control plane did not come back with a late executor: %v", err)
	}
	defer cp2.Stop()
	l := <-late
	if l.err != nil {
		t.Fatalf("late executor: %v", l.err)
	}
	defer l.ex.stop()

	dana2, err := client.Connect(url, minted.AdminCreds)
	if err != nil {
		t.Fatalf("reconnect admin: %v", err)
	}
	defer dana2.Close()
	waitForStateSeq(ctx, t, dana2, "orders", "invoice.invoice-1", birth.Seq)
	if _, err := dana2.CreateLog(ctx, "second", ""); err != nil {
		t.Fatalf("create log after restart: %v", err)
	}
	ops, err := dana2.Replay(ctx, "orders", "invoice.invoice-1")
	if err != nil || len(ops) != 1 {
		t.Fatalf("history after restart: %d ops, %v", len(ops), err)
	}
}

// TestStandaloneControlPlaneWritesBridgeProfile: with a GitHub App
// configured, the standalone plane serves the bridge and writes the
// hand-out `chronicle login` needs — public material only, naming the
// URL tenants connect to.
func TestStandaloneControlPlaneWritesBridgeProfile(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	b, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	srv, err := b.StartServer(-1)
	if err != nil {
		t.Fatalf("substrate: %v", err)
	}
	defer srv.Shutdown()
	url := srv.ClientURL()

	cp, err := fleet.StartControlPlane(ctx, fleet.ControlPlaneConfig{Dir: dir, URL: url, GithubClientID: "Iv1.standalone"})
	if err != nil {
		t.Fatalf("control plane with bridge: %v", err)
	}
	defer cp.Stop()

	data, err := os.ReadFile(filepath.Join(dir, "bridge.json"))
	if err != nil {
		t.Fatalf("bridge profile: %v", err)
	}
	var profile contract.BridgeProfile
	if err := json.Unmarshal(data, &profile); err != nil {
		t.Fatalf("decode bridge profile: %v", err)
	}
	if profile.URL != url || profile.GithubClientID != "Iv1.standalone" || profile.Sentinel == "" {
		t.Fatalf("bridge profile = %+v", profile)
	}
	if !strings.Contains(profile.Sentinel, "NATS USER JWT") {
		t.Fatalf("sentinel is not a creds file: %q", profile.Sentinel)
	}
}

// TestRunControlNeedsAURL: the unit's flag seam. No --url and no recorded
// url is a refusal that names both, not a dial of nothing.
func TestRunControlNeedsAURL(t *testing.T) {
	var out bytes.Buffer
	err := fleet.RunControl(context.Background(), []string{"--dir", t.TempDir()}, &out)
	if err == nil || !strings.Contains(err.Error(), "no --url") {
		t.Fatalf("RunControl without a url: %v", err)
	}
	if err := fleet.RunControl(context.Background(), []string{"--url", "nats://127.0.0.1:1", "extra"}, &out); err == nil || !strings.Contains(err.Error(), "positionals") {
		t.Fatalf("RunControl with a positional: %v", err)
	}
}
