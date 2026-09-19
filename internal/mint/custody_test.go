package mint_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/internal/mint"
)

// controlSubstrate boots a bootstrap server and connects its control user.
func controlSubstrate(t *testing.T) (string, *mint.Bootstrap, *nats.Conn) {
	t.Helper()
	b := testBootstrap(t)
	srv, err := b.StartServer(-1)
	if err != nil {
		t.Fatalf("substrate: %v", err)
	}
	t.Cleanup(srv.Shutdown)
	nc, err := mint.ConnectCreds(srv.ClientURL(), b.ControlCreds, "test-control")
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	t.Cleanup(nc.Close)
	return srv.ClientURL(), b, nc
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

// TestFleetTemplateFencesTheBucket is the fence on a real operator-mode
// server: a user carrying FleetTemplate can neither open, read, write, nor
// watch the AUTH bucket, while its other JetStream work succeeds; a user
// carrying ControlInstanceTemplate reads it.
func TestFleetTemplateFencesTheBucket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	url, b, ctrlConn := controlSubstrate(t)

	c, err := mint.CreateCustody(ctx, ctrlConn, 1)
	if err != nil {
		t.Fatalf("create custody: %v", err)
	}
	if _, err := c.PutOperator(ctx, mint.OperatorRecord{PublicKey: "OTEST", SigningSeed: "SOTEST"}, 0); err != nil {
		t.Fatalf("put operator: %v", err)
	}

	fleet, err := b.IssueControlUser("executor-test", mint.FleetTemplate())
	if err != nil {
		t.Fatalf("issue fleet user: %v", err)
	}
	fnc, err := mint.ConnectCreds(url, fleet.File, "fleet-test")
	if err != nil {
		t.Fatalf("connect fleet user: %v", err)
	}
	defer fnc.Close()

	// Every denied request is dropped by the server with an async
	// permission violation; the caller sees only a timeout. Short
	// deadlines keep the refusals cheap.
	short := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, 2*time.Second) }

	sctx, scancel := short()
	_, err = mint.OpenCustody(sctx, fnc)
	scancel()
	if err == nil {
		t.Fatal("fleet user opened the bucket")
	}
	if _, err := fnc.Request("$JS.API.DIRECT.GET.KV_AUTH.$KV.AUTH.operator", nil, 2*time.Second); err == nil {
		t.Fatal("fleet user read the operator entry by direct get")
	}
	fjs, err := jetstream.New(fnc)
	if err != nil {
		t.Fatalf("fleet jetstream: %v", err)
	}
	sctx, scancel = short()
	_, err = fjs.Publish(sctx, "$KV.AUTH.operator", []byte(`{"forged":true}`))
	scancel()
	if err == nil {
		t.Fatal("fleet user wrote the operator entry")
	}
	sctx, scancel = short()
	_, err = fjs.OrderedConsumer(sctx, "KV_AUTH", jetstream.OrderedConsumerConfig{})
	scancel()
	if err == nil {
		t.Fatal("fleet user opened a consumer on the bucket's stream")
	}
	if got, _, err := c.Operator(ctx); err != nil || got.SigningSeed != "SOTEST" {
		t.Fatalf("operator after the fleet user's attempts = %+v, %v", got, err)
	}

	// The fence is around one bucket, not a cage: the fleet user's own
	// JetStream work succeeds.
	scratch, err := fjs.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "FLEET_SCRATCH"})
	if err != nil {
		t.Fatalf("fleet user could not create its own bucket: %v", err)
	}
	if _, err := scratch.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("fleet user could not write its own bucket: %v", err)
	}

	// And a control instance reads.
	inst, err := b.IssueControlUser("instance-test", mint.ControlInstanceTemplate())
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
