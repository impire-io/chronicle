package mint_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/mint"
)

// controlSubstrate boots a root's server and connects the first instance's
// control user — the only user a root holds.
func controlSubstrate(t *testing.T) (string, *mint.Root, *nats.Conn) {
	t.Helper()
	r := testRoot(t)
	srv, err := r.B.StartServer(-1)
	if err != nil {
		t.Fatalf("substrate: %v", err)
	}
	t.Cleanup(srv.Shutdown)
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	nc, err := mint.ConnectCreds(srv.ClientURL(), bundle.ControlCreds, "test-control")
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	t.Cleanup(nc.Close)
	return srv.ClientURL(), r, nc
}

// TestCustodyRoundTripAndRevisionGuard is design 10's bucket on a real
// server: create, read back, and the compare-and-set that makes n control
// instances safe — eight writers from one revision, exactly one lands, the
// losers re-read and recompute.
func TestCustodyRoundTripAndRevisionGuard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, nc := controlSubstrate(t)

	if _, err := mint.OpenCustody(ctx, nc); !errors.Is(err, mint.ErrNotSealed) {
		t.Fatalf("open before seal = %v, want ErrNotSealed", err)
	}
	c, err := mint.CreateCustody(ctx, nc, 1)
	if err != nil {
		t.Fatalf("create custody: %v", err)
	}

	op := mint.OperatorRecord{PublicKey: "OTEST", SigningSeed: "SOTEST"}
	if _, err := c.PutOperator(ctx, op, 0); err != nil {
		t.Fatalf("put operator: %v", err)
	}
	got, rev, err := c.Operator(ctx)
	if err != nil || got != op || rev == 0 {
		t.Fatalf("operator = %+v rev %d, %v", got, rev, err)
	}
	if _, err := c.PutOperator(ctx, op, 0); !errors.Is(err, mint.ErrRevisionMismatch) {
		t.Fatalf("second create = %v, want ErrRevisionMismatch", err)
	}
	if _, _, err := c.Tenant(ctx, "nobody"); !errors.Is(err, mint.ErrNoRecord) {
		t.Fatalf("missing tenant = %v, want ErrNoRecord", err)
	}

	// The race: eight instances mutate one tenant from the same revision.
	rev0, err := c.PutTenant(ctx, mint.TenantRecord{Name: "acme", PublicKey: "AACME", JWT: "v1"}, 0)
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.PutTenant(ctx, mint.TenantRecord{Name: "acme", PublicKey: "AACME", JWT: fmt.Sprintf("v2-from-%d", i)}, rev0)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	wins, losses := 0, 0
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, mint.ErrRevisionMismatch):
			losses++
		default:
			t.Fatalf("racing write failed for another reason: %v", err)
		}
	}
	if wins != 1 || losses != 7 {
		t.Fatalf("race: %d wins, %d losses", wins, losses)
	}
	// The loser's protocol: re-read, recompute on top, write at the
	// current revision.
	cur, rev, err := c.Tenant(ctx, "acme")
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	cur.JWT += "+retry"
	if _, err := c.PutTenant(ctx, cur, rev); err != nil {
		t.Fatalf("retry at revision %d: %v", rev, err)
	}
	final, _, _ := c.Tenant(ctx, "acme")
	if final.JWT[:8] != "v2-from-" || final.JWT[len(final.JWT)-6:] != "+retry" {
		t.Fatalf("final JWT %q is not the winner plus the retry", final.JWT)
	}

	names, err := c.Tenants(ctx)
	if err != nil || len(names) != 1 || names[0] != "acme" {
		t.Fatalf("tenants = %v, %v", names, err)
	}
	entries, err := c.Entries(ctx)
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries = %d, %v", len(entries), err)
	}
	if _, err := mint.OpenCustody(ctx, nc); err != nil {
		t.Fatalf("open after create: %v", err)
	}
}

// TestRoleTemplatesFenceTheBucket is the bucket half of the fence on a
// real operator-mode server: a user under the executor, workloads, or cli
// template can neither open, read, write, nor watch the AUTH bucket; the
// workloads user's own JetStream work — its META bucket — succeeds; and a
// control instance reads. The rest of each role's allow-list is the fleet
// package's to verify against running services.
func TestRoleTemplatesFenceTheBucket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	url, r, ctrlConn := controlSubstrate(t)

	c, err := mint.CreateCustody(ctx, ctrlConn, 1)
	if err != nil {
		t.Fatalf("create custody: %v", err)
	}
	if _, err := c.PutOperator(ctx, mint.OperatorRecord{PublicKey: "OTEST", SigningSeed: "SOTEST"}, 0); err != nil {
		t.Fatalf("put operator: %v", err)
	}

	// Every denied request is dropped by the server with an async
	// permission violation; the caller sees only a timeout. Short
	// deadlines keep the refusals cheap.
	short := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, 2*time.Second) }
	fenced := func(name string, limits jwt.UserPermissionLimits) *nats.Conn {
		t.Helper()
		user, err := r.B.IssueControlUser(name, limits)
		if err != nil {
			t.Fatalf("issue %s: %v", name, err)
		}
		nc, err := mint.ConnectCreds(url, user.File, name)
		if err != nil {
			t.Fatalf("connect %s: %v", name, err)
		}
		t.Cleanup(nc.Close)
		sctx, scancel := short()
		_, err = mint.OpenCustody(sctx, nc)
		scancel()
		if err == nil {
			t.Fatalf("%s opened the bucket", name)
		}
		if _, err := nc.Request("$JS.API.DIRECT.GET.KV_AUTH.$KV.AUTH.operator", nil, 2*time.Second); err == nil {
			t.Fatalf("%s read the operator entry by direct get", name)
		}
		if _, err := nc.Request("$KV.AUTH.operator", []byte(`{"forged":true}`), 2*time.Second); err == nil {
			t.Fatalf("%s wrote the operator entry", name)
		}
		return nc
	}
	fenced("exec-test", mint.ExecutorTemplate("exec-test"))
	fenced("cli-test", mint.CLITemplate())
	wl := fenced("workloads-test", mint.WorkloadsTemplate())
	if got, _, err := c.Operator(ctx); err != nil || got.SigningSeed != "SOTEST" {
		t.Fatalf("operator after the fenced users' attempts = %+v, %v", got, err)
	}

	// The fence is around one bucket, not a cage: the workload service's
	// own JetStream work — its META bucket in the control account — is
	// untouched, while a consumer on the bucket's stream is refused.
	wjs, err := jetstream.New(wl)
	if err != nil {
		t.Fatalf("workloads jetstream: %v", err)
	}
	meta, err := wjs.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: contract.MetaBucket})
	if err != nil {
		t.Fatalf("workloads user could not create its META bucket: %v", err)
	}
	if _, err := meta.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("workloads user could not write its META bucket: %v", err)
	}
	sctx, scancel := short()
	_, err = wjs.OrderedConsumer(sctx, "KV_AUTH", jetstream.OrderedConsumerConfig{})
	scancel()
	if err == nil {
		t.Fatal("workloads user opened a consumer on the bucket's stream")
	}

	// And a control instance reads.
	inst, err := r.B.IssueControlUser("instance-test", mint.ControlInstanceTemplate())
	if err != nil {
		t.Fatalf("issue instance user: %v", err)
	}
	inc, err := mint.ConnectCreds(url, inst.File, "instance-test")
	if err != nil {
		t.Fatalf("connect instance user: %v", err)
	}
	defer inc.Close()
	c2, err := mint.OpenCustody(ctx, inc)
	if err != nil {
		t.Fatalf("instance user could not open the bucket: %v", err)
	}
	if got, _, err := c2.Operator(ctx); err != nil || got.SigningSeed != "SOTEST" {
		t.Fatalf("instance user read = %+v, %v", got, err)
	}
}
