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
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/internal/workloads"
)

// standaloneExecutor is chronicle-executor's shape in-process: its own
// executor-template credential, the in-process backend, its instance name
// as its id.
type standaloneExecutor struct {
	nc *nats.Conn
	ex *executor.Executor
}

func startStandaloneExecutor(ctx context.Context, url string, creds []byte) (*standaloneExecutor, error) {
	id, err := mint.InstanceOf(creds)
	if err != nil {
		return nil, err
	}
	nc, err := mint.ConnectCreds(url, creds, id)
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

// standaloneWorkloads is chronicle-workloads' shape in-process: its own
// workloads-template credential and the service.
type standaloneWorkloads struct {
	nc  *nats.Conn
	svc *workloads.Service
}

func startStandaloneWorkloads(ctx context.Context, url string, creds []byte) (*standaloneWorkloads, error) {
	nc, err := mint.ConnectCreds(url, creds, "chronicle-workloads")
	if err != nil {
		return nil, err
	}
	svc, err := workloads.Start(ctx, nc, workloads.Config{ScanEvery: 500 * time.Millisecond})
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &standaloneWorkloads{nc: nc, svc: svc}, nil
}

func (s *standaloneWorkloads) stop() {
	s.svc.Stop()
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

// issueMembers issues the hosted fleet's other members over the sealed
// bucket — a workload service, an executor host, and an operator's CLI —
// through the first instance's driver, as `operator instance add` does.
func issueMembers(ctx context.Context, t *testing.T, url string, r *mint.Root) (wl, ex, cli []byte) {
	t.Helper()
	sysConn, _, custody := natstest.OpenInstance(t, url, r)
	driver, err := mint.NewJWTDriver(ctx, custody, sysConn, url)
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	add := func(name string, tpl mint.Template) []byte {
		b, err := driver.AddInstance(ctx, name, tpl)
		if err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		return b.ControlCreds
	}
	return add("wl-1", mint.TemplateWorkloads), add("host-1", mint.TemplateExecutor), add("cli-1", mint.TemplateCLI)
}

// TestStandaloneControlPlane is design 09's stand-up ceremony in
// miniature, in the shape 0031 split it into: the substrate serves and is
// sealed first; the workload service, a control instance from its bundle
// and a URL, and an executor host each join over the wire with a
// credential of their own role; a mint ends the way onboarding demands —
// the tenant's node placed and answering. Then the hosted boot order:
// everything but the substrate restarts, the workload service and control
// first and the executor a beat later, and the boot replay must converge
// through the level scan rather than fail the unit.
func TestStandaloneControlPlane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	url, r := natstest.StartSealedOperator(t)
	wlCreds, exCreds, cliCreds := issueMembers(ctx, t, url, r)

	wl, err := startStandaloneWorkloads(ctx, url, wlCreds)
	if err != nil {
		t.Fatalf("workload service: %v", err)
	}
	wlStopped := false
	defer func() {
		if !wlStopped {
			wl.stop()
		}
	}()
	cp, err := fleet.StartControlPlane(ctx, fleet.ControlPlaneConfig{URL: url, Bundle: r.BundleDir("instance-1")})
	if err != nil {
		t.Fatalf("control plane: %v", err)
	}
	if cp.Instance != "instance-1" {
		t.Fatalf("control plane instance = %q", cp.Instance)
	}
	stopped := false
	defer func() {
		if !stopped {
			cp.Stop()
		}
	}()
	ex, err := startStandaloneExecutor(ctx, url, exCreds)
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	exStopped := false
	defer func() {
		if !exStopped {
			ex.stop()
		}
	}()

	// The mint probe, as the operator's CLI runs it: mint, connect, read.
	ctrl, err := client.ConnectControlCreds(url, cliCreds)
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

	// The hosted boot order. Control's replay dispatches the tenant in
	// custody before any executor is registered; the executor arrives a
	// second later and the scan must hand it the slot.
	cp.Stop()
	stopped = true
	ex.stop()
	exStopped = true
	wl.stop()
	wlStopped = true

	wl2, err := startStandaloneWorkloads(ctx, url, wlCreds)
	if err != nil {
		t.Fatalf("workload service did not come back: %v", err)
	}
	defer wl2.stop()
	type lateStart struct {
		ex  *standaloneExecutor
		err error
	}
	late := make(chan lateStart, 1)
	go func() {
		time.Sleep(time.Second)
		ex, err := startStandaloneExecutor(ctx, url, exCreds)
		late <- lateStart{ex, err}
	}()
	cp2, err := fleet.StartControlPlane(ctx, fleet.ControlPlaneConfig{URL: url, Bundle: r.BundleDir("instance-1")})
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
// configured, the instance serves the bridge from custody and writes the
// hand-out `chronicle login` needs where it is told — public material
// only, naming the URL tenants connect to.
func TestStandaloneControlPlaneWritesBridgeProfile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	url, r := natstest.StartSealedOperator(t)
	profile := filepath.Join(t.TempDir(), "bridge.json")

	cp, err := fleet.StartControlPlane(ctx, fleet.ControlPlaneConfig{URL: url, Bundle: r.BundleDir("instance-1"), GithubClientID: "Iv1.standalone", BridgeProfile: profile})
	if err != nil {
		t.Fatalf("control plane with bridge: %v", err)
	}
	defer cp.Stop()

	data, err := os.ReadFile(profile)
	if err != nil {
		t.Fatalf("bridge profile: %v", err)
	}
	var p contract.BridgeProfile
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("decode bridge profile: %v", err)
	}
	if p.URL != url || p.GithubClientID != "Iv1.standalone" || p.Sentinel == "" {
		t.Fatalf("bridge profile = %+v", p)
	}
	if !strings.Contains(p.Sentinel, "NATS USER JWT") {
		t.Fatalf("sentinel is not a creds file: %q", p.Sentinel)
	}
}

// TestRunControlFlags: the unit's flag seam. No --url or no --bundle is a
// refusal that names both; a bundle without sys.creds is refused by name.
func TestRunControlFlags(t *testing.T) {
	var out bytes.Buffer
	err := fleet.RunControl(context.Background(), []string{"--bundle", t.TempDir()}, &out)
	if err == nil || !strings.Contains(err.Error(), "--url and --bundle") {
		t.Fatalf("RunControl without a url: %v", err)
	}
	if err := fleet.RunControl(context.Background(), []string{"--url", "nats://127.0.0.1:1", "--bundle", t.TempDir(), "extra"}, &out); err == nil || !strings.Contains(err.Error(), "positionals") {
		t.Fatalf("RunControl with a positional: %v", err)
	}
	fleetBundle := t.TempDir()
	if err := os.WriteFile(filepath.Join(fleetBundle, "control.creds"), []byte("not a creds file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fleet.RunControl(context.Background(), []string{"--url", "nats://127.0.0.1:1", "--bundle", fleetBundle}, &out); err == nil || !strings.Contains(err.Error(), "not a control instance") {
		t.Fatalf("RunControl with a one-file bundle: %v", err)
	}
}
