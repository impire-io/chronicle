package fleet_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/executor"
	"github.com/impire-io/chronicle/internal/fleet"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/internal/workloads"
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
	if err != nil || !b2.IsControlInstance() {
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

	// An executor instance: one file, its own subjects, never the bucket,
	// never a verb.
	if out, err := run("add", "worker-1", "--template", "executor"); err != nil {
		t.Fatalf("add worker-1: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(r.BundleDir("worker-1"), "sys.creds")); !os.IsNotExist(err) {
		t.Fatalf("an executor bundle holds a SYS user (%v)", err)
	}
	w, err := mint.ReadBundle(r.BundleDir("worker-1"))
	if err != nil || w.IsControlInstance() {
		t.Fatalf("worker-1's bundle: %+v, %v", w, err)
	}
	if issued, err := mint.InstanceOf(w.ControlCreds); err != nil || issued != "worker-1" {
		t.Fatalf("worker-1's credential names %q, %v", issued, err)
	}
	assertFenced(t, url, w.ControlCreds, "worker-1")
	wnc, err := mint.ConnectCreds(url, w.ControlCreds, "worker-1")
	if err != nil {
		t.Fatalf("executor user connects: %v", err)
	}
	// Its own creds pull reaches control (nothing is assigned, so the
	// record refuses); a verb does not.
	if _, err := executor.PullCreds(ctx, wnc, "worker-1", "acme", contract.WorkloadNodeName); err == nil || !strings.Contains(err.Error(), "not-assigned") {
		t.Fatalf("the executor's own creds pull did not reach control: %v", err)
	}
	wnc.Close()
	if wc, err := client.ConnectControlCreds(url, w.ControlCreds); err == nil {
		mctx, mcancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := wc.MintTenant(mctx, "beta", "")
		mcancel()
		wc.Close()
		if err == nil {
			t.Fatal("an executor's credential reached the mint verb")
		}
	}

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
		t.Fatal("a removed executor still connects")
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

// TestRoleTemplatesHold is the fence per role against running services
// (design 10 § the fence, 0032): each role's own work succeeds and what
// its row forbids is refused — an executor registers, reports and pulls
// creds on its own subjects only and reaches no verb and no stream; the
// workload service runs the fleet log and cannot mint or pull creds; the
// CLI mints and cannot touch JetStream; a payload naming another executor
// is refused by the serving side.
func TestRoleTemplatesHold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	url, r := natstest.StartSealedOperator(t)
	sysConn, ctrlConn, custody := natstest.OpenInstance(t, url, r)
	driver, err := mint.NewJWTDriver(ctx, custody, sysConn, url)
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	svc, err := control.Start(ctrlConn, control.Config{Driver: driver, URL: url})
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	instance := func(name string, tpl mint.Template) *nats.Conn {
		t.Helper()
		b, err := driver.AddInstance(ctx, name, tpl)
		if err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		nc, err := mint.ConnectCreds(url, b.ControlCreds, name)
		if err != nil {
			t.Fatalf("connect %s: %v", name, err)
		}
		t.Cleanup(nc.Close)
		return nc
	}
	short := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, 2*time.Second) }
	refused := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: the fence let this through", name)
		}
	}
	mintAs := func(nc *nats.Conn, tenant string) error {
		sctx, scancel := short()
		defer scancel()
		data, _ := json.Marshal(client.TenantMintRequest{Name: tenant})
		msg, err := nc.RequestWithContext(sctx, client.TenantMintSubject, data)
		if err != nil {
			return err
		}
		if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
			return errors.New(code)
		}
		return nil
	}
	jetstreamAs := func(nc *nats.Conn) error {
		js, err := jetstream.New(nc)
		if err != nil {
			return err
		}
		sctx, scancel := short()
		defer scancel()
		_, err = js.AccountInfo(sctx)
		return err
	}

	// The workload service, under its own role, runs the fleet log.
	wl := instance("wl-1", mint.TemplateWorkloads)
	wsvc, err := workloads.Start(ctx, wl, workloads.Config{ScanEvery: 150 * time.Millisecond, AuctionWindow: 80 * time.Millisecond})
	if err != nil {
		t.Fatalf("workloads under its template: %v", err)
	}
	t.Cleanup(wsvc.Stop)
	// Control dispatches, as it does at every mint; the service under its
	// template serves. The service itself never publishes a dispatch.
	if _, err := workloads.Dispatch(ctx, ctrlConn, contract.FleetDispatchRequest{Tenant: "acme", Workload: contract.WorkloadNodeName, Kind: contract.WorkloadKindNode}); err != nil {
		t.Fatalf("the workload service under its template does not serve dispatch: %v", err)
	}
	sctx, scancel := short()
	_, err = workloads.Dispatch(sctx, wl, contract.FleetDispatchRequest{Tenant: "beta", Workload: contract.WorkloadNodeName, Kind: contract.WorkloadKindNode})
	scancel()
	refused("workloads dispatches", err)
	refused("workloads mints", mintAs(wl, "rogue-wl"))
	sctx, scancel = short()
	_, err = executor.PullCreds(sctx, wl, "exec-a", "acme", contract.WorkloadNodeName)
	scancel()
	refused("workloads pulls creds", err)

	// Executors: each on its own subjects, nothing else.
	a := instance("exec-a", mint.TemplateExecutor)
	instance("exec-b", mint.TemplateExecutor)
	if _, err := workloads.Register(ctx, a, contract.FleetRegisterRequest{Executor: "exec-a", Backend: "test"}); err != nil {
		t.Fatalf("exec-a cannot register as itself: %v", err)
	}
	sctx, scancel = short()
	_, err = workloads.Register(sctx, a, contract.FleetRegisterRequest{Executor: "exec-b", Backend: "test"})
	scancel()
	refused("exec-a registers as exec-b", err)
	if _, err := workloads.Report(ctx, a, contract.FleetReportRequest{Executor: "exec-a", Tenant: "acme", Workload: contract.WorkloadNodeName, Slot: "0", Reason: contract.ReleaseFailing}); err != nil {
		t.Fatalf("exec-a cannot report as itself: %v", err)
	}
	// Its own creds pull reaches control — the record refuses, since the
	// auction has not placed anything on exec-a — and a pull naming
	// another executor is refused before the record, on the subject or
	// in the payload.
	if _, err := executor.PullCreds(ctx, a, "exec-a", "acme", contract.WorkloadNodeName); err == nil || !strings.Contains(err.Error(), "not-assigned") {
		t.Fatalf("exec-a's own pull: %v, want not-assigned", err)
	}
	sctx, scancel = short()
	_, err = executor.PullCreds(sctx, a, "exec-b", "acme", contract.WorkloadNodeName)
	scancel()
	refused("exec-a pulls on exec-b's subject", err)
	forged, _ := json.Marshal(contract.FleetCredsRequest{Executor: "exec-b", Tenant: "acme", Workload: contract.WorkloadNodeName})
	msg, err := a.RequestWithContext(ctx, contract.FleetCredsSubject("exec-a"), forged)
	if err != nil {
		t.Fatalf("forged pull: %v", err)
	}
	if code := msg.Header.Get(micro.ErrorCodeHeader); code != "caller-mismatch" {
		t.Fatalf("forged pull answered with %q, want caller-mismatch", code)
	}
	refused("exec-a mints", mintAs(a, "rogue-exec"))
	refused("exec-a reaches JetStream", jetstreamAs(a))
	sctx, scancel = short()
	_, err = a.RequestWithContext(sctx, contract.FleetDispatchSubject, []byte(`{}`))
	scancel()
	refused("exec-a dispatches", err)

	// The CLI: the verbs and nothing on JetStream.
	c := instance("cli-1", mint.TemplateCLI)
	if err := mintAs(c, "acme-cli"); err != nil {
		t.Fatalf("the cli cannot mint: %v", err)
	}
	refused("cli reaches JetStream", jetstreamAs(c))
	sctx, scancel = short()
	_, err = executor.PullCreds(sctx, c, "exec-a", "acme", contract.WorkloadNodeName)
	scancel()
	refused("cli pulls creds", err)
}

// TestRotationLiveWithBothKeysTrusted is design 10 § rotation, the two
// steps as the environment and the service take them: the environment
// adds the new signing key to the operator JWT and rolls the node; the
// service's `rotate-signing-key --url --bundle --new-signing-seed` lands
// the seed and re-signs every account by compare-and-set then push, with
// a mint interleaved that signs under the new key; the environment removes
// the old key and rolls again; every credential the install holds still
// connects. Before the environment's first step, the service's step is
// refused and the bucket is untouched.
func TestRotationLiveWithBothKeysTrusted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	r, err := mint.InitRoot(t.TempDir())
	if err != nil {
		t.Fatalf("init root: %v", err)
	}
	b := r.B
	key, err := r.NodeKey("embedded")
	if err != nil {
		t.Fatalf("node key: %v", err)
	}
	// The environment's node, rolled by restarting it on the same store
	// under whatever operator JWT the root holds at the time.
	var url string
	var stop func()
	roll := func() {
		t.Helper()
		if stop != nil {
			stop()
		}
		srv, err := b.StartServerWithKey(-1, key)
		if err != nil {
			t.Fatalf("start node: %v", err)
		}
		url = srv.ClientURL()
		stop = func() {
			srv.Shutdown()
			srv.WaitForShutdown()
		}
	}
	roll()
	defer func() { stop() }()
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	sealSys, err := mint.ConnectCreds(url, bundle.SysCreds, "seal-sys")
	if err != nil {
		t.Fatalf("connect for seal: %v", err)
	}
	sealConn, err := mint.ConnectCreds(url, bundle.ControlCreds, "seal")
	if err != nil {
		t.Fatalf("connect for seal: %v", err)
	}
	if _, err := r.Seal(ctx, sealSys, sealConn, mint.SealOptions{Replicas: 1}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	sealConn.Close()
	sealSys.Close()

	// A tenant and a member under the old key.
	driverAt := func(url string) *mint.JWTDriver {
		t.Helper()
		sysConn, err := mint.ConnectCreds(url, bundle.SysCreds, "sys")
		if err != nil {
			t.Fatalf("sys: %v", err)
		}
		t.Cleanup(sysConn.Close)
		ctrlConn, err := mint.ConnectCreds(url, bundle.ControlCreds, "control")
		if err != nil {
			t.Fatalf("control: %v", err)
		}
		t.Cleanup(ctrlConn.Close)
		c, err := mint.OpenCustody(ctx, ctrlConn)
		if err != nil {
			t.Fatalf("custody: %v", err)
		}
		d, err := mint.NewJWTDriver(ctx, c, sysConn, url)
		if err != nil {
			t.Fatalf("driver: %v", err)
		}
		return d
	}
	acme, err := driverAt(url).MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint acme: %v", err)
	}
	dana, err := mint.IssueMember(acme, "dana")
	if err != nil {
		t.Fatalf("issue dana: %v", err)
	}
	connects := func(name string, creds []byte) {
		t.Helper()
		nc, err := mint.ConnectCreds(url, creds, name)
		if err != nil {
			t.Fatalf("%s cannot connect: %v", name, err)
		}
		nc.Close()
	}

	// The new key, not yet trusted: the service's step is refused and
	// the bucket still names the old key.
	newKP, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatal(err)
	}
	newSeed, _ := newKP.Seed()
	newPub, _ := newKP.PublicKey()
	seedFile := filepath.Join(t.TempDir(), "new.nk")
	if err := os.WriteFile(seedFile, newSeed, 0o600); err != nil {
		t.Fatal(err)
	}
	rotate := func() error {
		var out bytes.Buffer
		return fleet.RotateSigningKey(ctx, []string{"--url", url, "--bundle", r.BundleDir("instance-1"), "--new-signing-seed", seedFile}, &out)
	}
	if err := rotate(); err == nil || !strings.Contains(err.Error(), "do not trust") {
		t.Fatalf("rotation before the environment's step: %v, want a refusal naming the trust", err)
	}
	c0, err := mint.OpenCustody(ctx, mustConnect(t, url, bundle.ControlCreds, "check"))
	if err != nil {
		t.Fatal(err)
	}
	if op, _, _ := c0.Operator(ctx); op.PublicKey == newPub {
		t.Fatal("a refused rotation changed the operator entry")
	}

	// Step one, the environment's: both keys trusted, the node rolled.
	oc, err := jwt.DecodeOperatorClaims(b.OperatorJWT)
	if err != nil {
		t.Fatal(err)
	}
	okp, err := nkeys.FromSeed(b.OperatorSeed)
	if err != nil {
		t.Fatal(err)
	}
	oldPub := oc.SigningKeys[0]
	oc.SigningKeys = jwt.StringList{oldPub, newPub}
	if b.OperatorJWT, err = oc.Encode(okp); err != nil {
		t.Fatal(err)
	}
	roll()

	// Step two, the service's — with a mint interleaved: beta is signed
	// under whichever key the bucket names, and connects either way.
	if err := rotate(); err != nil {
		t.Fatalf("rotation with both keys trusted: %v", err)
	}
	beta, err := driverAt(url).MintAccount(ctx, "beta")
	if err != nil {
		t.Fatalf("mint mid-roll: %v", err)
	}
	erin, err := mint.IssueMember(beta, "erin")
	if err != nil {
		t.Fatal(err)
	}
	c1, err := mint.OpenCustody(ctx, mustConnect(t, url, bundle.ControlCreds, "check-2"))
	if err != nil {
		t.Fatal(err)
	}
	if op, _, _ := c1.Operator(ctx); op.PublicKey != newPub {
		t.Fatalf("the bucket names %s, want the new key %s", op.PublicKey, newPub)
	}
	for _, name := range []string{"SYS", "CONTROL"} {
		rec, _, err := c1.Account(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		ac, _ := jwt.DecodeAccountClaims(rec.JWT)
		if ac.Issuer != newPub {
			t.Fatalf("%s is signed by %s, want %s", name, ac.Issuer, newPub)
		}
	}
	connects("dana", dana.File)
	connects("erin", erin.File)

	// Step three, the environment's: the old key gone, the node rolled;
	// everything the install holds still connects.
	oc.SigningKeys = jwt.StringList{newPub}
	if b.OperatorJWT, err = oc.Encode(okp); err != nil {
		t.Fatal(err)
	}
	roll()
	connects("dana after", dana.File)
	connects("erin after", erin.File)
	connects("instance-1 control", bundle.ControlCreds)
	connects("instance-1 sys", bundle.SysCreds)
	if _, err := driverAt(url).MintAccount(ctx, "gamma"); err != nil {
		t.Fatalf("mint after the roll: %v", err)
	}
}

func mustConnect(t *testing.T, url string, creds []byte, name string) *nats.Conn {
	t.Helper()
	nc, err := mint.ConnectCreds(url, creds, name)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	t.Cleanup(nc.Close)
	return nc
}
