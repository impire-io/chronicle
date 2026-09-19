package mint_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/natstest"
)

func startDriver(t *testing.T) (url string, d *mint.JWTDriver) {
	t.Helper()
	url, r := natstest.StartSealedOperator(t)
	return url, driverOver(t, url, r)
}

// driverOver builds one control instance's driver over the sealed bucket —
// call it twice for two instances sharing custody.
func driverOver(t *testing.T, url string, r *mint.Root) *mint.JWTDriver {
	t.Helper()
	sysConn, _, c := natstest.OpenInstance(t, url, r)
	d, err := mint.NewJWTDriver(context.Background(), c, sysConn, url)
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	return d
}

func TestMintAccountAndVerifyByConnecting(t *testing.T) {
	url, d := startDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if acct.PublicKey == "" || len(acct.SigningSeed) == 0 || len(acct.ScopedSeed) == 0 {
		t.Fatalf("mint returned incomplete material: %+v", acct)
	}

	// The service user holds account-default rights: it owns the account's
	// JetStream internals.
	svc, err := mint.IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		t.Fatalf("issue service user: %v", err)
	}
	nc, err := mint.ConnectCreds(url, svc.File, "test-service")
	if err != nil {
		t.Fatalf("connect service user: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateStream(ctx, contract.LogStreamConfig("orders", 0, "")); err != nil {
		t.Fatalf("service user must create streams: %v", err)
	}
}

func TestMemberBaseline(t *testing.T) {
	url, d := startDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	// The service side prepares what the member will touch: the log stream
	// and the META bucket.
	svc, err := mint.IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		t.Fatalf("issue service user: %v", err)
	}
	svcConn, err := mint.ConnectCreds(url, svc.File, "test-service")
	if err != nil {
		t.Fatalf("connect service: %v", err)
	}
	defer svcConn.Close()
	sjs, err := jetstream.New(svcConn)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := sjs.CreateStream(ctx, contract.LogStreamConfig("orders", 0, "")); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	meta, err := sjs.CreateKeyValue(ctx, contract.MetaBucketConfig())
	if err != nil {
		t.Fatalf("create META: %v", err)
	}
	if _, err := meta.Put(ctx, contract.MetaLogConfig("orders"), []byte(`{"status":"active"}`)); err != nil {
		t.Fatalf("seed META: %v", err)
	}

	member, err := mint.IssueMember(acct, "alice")
	if err != nil {
		t.Fatalf("issue member: %v", err)
	}

	var mu sync.Mutex
	var violations []string
	mnc, err := mint.ConnectCreds(url, member.File, "test-member")
	if err != nil {
		t.Fatalf("connect member: %v", err)
	}
	defer mnc.Close()
	mnc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		mu.Lock()
		defer mu.Unlock()
		violations = append(violations, err.Error())
	})
	mjs, err := jetstream.New(mnc)
	if err != nil {
		t.Fatalf("member jetstream: %v", err)
	}

	// Positive: a member appends — the direct JetStream publish of 0008.
	msg := nats.NewMsg(contract.OpsSubject("orders", "invoice-1"))
	msg.Header = contract.Op{ID: "op-1", Type: contract.OpTypeSnapshot, Author: "alice"}.Header()
	msg.Header.Set(contract.HdrExpectedLastSubjSeq, "0")
	msg.Data = []byte(`{"state":{},"frontier":[]}`)
	ack, err := mjs.PublishMsg(ctx, msg)
	if err != nil {
		t.Fatalf("member append refused: %v", err)
	}

	// Positive: a member replays through an ordered consumer.
	stream, err := mjs.Stream(ctx, contract.StreamName("orders"))
	if err != nil {
		t.Fatalf("member stream handle: %v", err)
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{contract.OpsFilter("orders")},
	})
	if err != nil {
		t.Fatalf("member ordered consumer: %v", err)
	}
	got, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("member replay: %v", err)
	}
	if md, err := got.Metadata(); err != nil || md.Sequence.Stream != ack.Sequence {
		t.Fatalf("replay returned wrong message: %v %v", md, err)
	}

	// Positive: a member reads META (the pre-flight path).
	mmeta, err := mjs.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		t.Fatalf("member META handle: %v", err)
	}
	if _, err := mmeta.Get(ctx, contract.MetaLogConfig("orders")); err != nil {
		t.Fatalf("member META read: %v", err)
	}

	// Negative: a member cannot touch the account's stream internals —
	// the 0003 ownership boundary held at the wire. The publish to
	// $JS.API is dropped by permissions, so the request times out.
	shortCtx, cancelShort := context.WithTimeout(ctx, 2*time.Second)
	defer cancelShort()
	if err := mjs.DeleteStream(shortCtx, contract.StreamName("orders")); err == nil {
		t.Fatal("member deleted a stream: the baseline is broken")
	}
	shortCtx2, cancelShort2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancelShort2()
	if _, err := mjs.CreateStream(shortCtx2, contract.LogStreamConfig("rogue", 0, "")); err == nil {
		t.Fatal("member created a stream: the baseline is broken")
	}

	// Negative: a member cannot write META.
	shortCtx3, cancelShort3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancelShort3()
	if _, err := mmeta.Put(shortCtx3, contract.MetaLogConfig("orders"), []byte(`{}`)); err == nil {
		t.Fatal("member wrote META: the baseline is broken")
	}

	// Negative: a member cannot publish outside CHRON.>.
	if err := mnc.Publish("outside.chron", []byte("x")); err != nil {
		t.Fatalf("publish returned sync error: %v", err)
	}
	if err := mnc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(violations)
		mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, v := range violations {
		if strings.Contains(strings.ToLower(v), "permissions violation") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a permissions violation for a non-CHRON publish, got %v", violations)
	}
}

// waitDisconnected polls until the connection loses the server or the
// deadline passes — eviction arrives as a server-side close, and the
// client parks in reconnect.
func waitDisconnected(t *testing.T, nc *nats.Conn) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for nc.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("connection still up: eviction did not land")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRevokeUserEvictsAndBlocks(t *testing.T) {
	url, d := startDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	alice, err := mint.IssueMember(acct, "alice")
	if err != nil {
		t.Fatalf("issue member: %v", err)
	}
	bob, err := mint.IssueMember(acct, "bob")
	if err != nil {
		t.Fatalf("issue second member: %v", err)
	}

	anc, err := mint.ConnectCreds(url, alice.File, "alice")
	if err != nil {
		t.Fatalf("connect alice: %v", err)
	}
	defer anc.Close()
	bnc, err := mint.ConnectCreds(url, bob.File, "bob")
	if err != nil {
		t.Fatalf("connect bob: %v", err)
	}
	defer bnc.Close()

	if err := d.RevokeUser(ctx, "acme", alice.PublicKey); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	// The revoked connection is actively closed; a fresh dial with the
	// same creds is refused.
	waitDisconnected(t, anc)
	if nc, err := mint.ConnectCreds(url, alice.File, "alice-again"); err == nil {
		nc.Close()
		t.Fatal("revoked creds reconnected: revocation is not enforced")
	}

	// Revocation is surgical: the other member never blinks.
	if !bnc.IsConnected() {
		t.Fatal("unrevoked member lost its connection")
	}
	if _, err := mint.ConnectCreds(url, bob.File, "bob-again"); err != nil {
		t.Fatalf("unrevoked member refused a fresh dial: %v", err)
	}
}

func TestRotateScopedSignerEvictsMembersKeepsService(t *testing.T) {
	url, d := startDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	svc, err := mint.IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		t.Fatalf("issue service: %v", err)
	}
	alice, err := mint.IssueMember(acct, "alice")
	if err != nil {
		t.Fatalf("issue member: %v", err)
	}

	snc, err := mint.ConnectCreds(url, svc.File, "svc")
	if err != nil {
		t.Fatalf("connect service: %v", err)
	}
	defer snc.Close()
	anc, err := mint.ConnectCreds(url, alice.File, "alice")
	if err != nil {
		t.Fatalf("connect alice: %v", err)
	}
	defer anc.Close()

	newSeed, err := d.RotateScopedSigner(ctx, "acme")
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Every member the old key signed is out — evicted and refused.
	waitDisconnected(t, anc)
	if nc, err := mint.ConnectCreds(url, alice.File, "alice-again"); err == nil {
		nc.Close()
		t.Fatal("old-key member reconnected: rotation did not evict")
	}

	// The service user rides through: its issuer, the plain signing key,
	// stays in the account.
	if !snc.IsConnected() {
		t.Fatal("service user lost its connection during member rekey")
	}

	// Creds under the new scoped key work the moment the push returns —
	// and custody already holds that key for the next instance to read.
	rekeyed, err := d.Tenant(ctx, "acme")
	if err != nil || string(rekeyed.ScopedSeed) != string(newSeed) {
		t.Fatalf("custody after rekey: %+v, %v", rekeyed, err)
	}
	alice2, err := mint.IssueMember(rekeyed, "alice")
	if err != nil {
		t.Fatalf("re-issue member: %v", err)
	}
	nc2, err := mint.ConnectCreds(url, alice2.File, "alice-rekeyed")
	if err != nil {
		t.Fatalf("re-issued member refused: %v", err)
	}
	nc2.Close()
}

func TestDedupAndBirthGuardAreServerEnforced(t *testing.T) {
	url, d := startDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	svc, err := mint.IssueServiceUser(acct, "svc")
	if err != nil {
		t.Fatalf("issue service: %v", err)
	}
	svcConn, err := mint.ConnectCreds(url, svc.File, "svc")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer svcConn.Close()
	sjs, err := jetstream.New(svcConn)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := sjs.CreateStream(ctx, contract.LogStreamConfig("orders", 0, "")); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	member, err := mint.IssueMember(acct, "alice")
	if err != nil {
		t.Fatalf("issue member: %v", err)
	}
	mnc, err := mint.ConnectCreds(url, member.File, "alice")
	if err != nil {
		t.Fatalf("connect member: %v", err)
	}
	defer mnc.Close()
	mjs, err := jetstream.New(mnc)
	if err != nil {
		t.Fatalf("member jetstream: %v", err)
	}

	birth := func(id string) (*jetstream.PubAck, error) {
		msg := nats.NewMsg(contract.OpsSubject("orders", "invoice-1"))
		msg.Header = contract.Op{ID: id, Type: contract.OpTypeSnapshot, Author: "alice"}.Header()
		msg.Header.Set(contract.HdrExpectedLastSubjSeq, "0")
		snap, _ := json.Marshal(contract.Snapshot{State: []byte(`{}`)})
		msg.Data = snap
		return mjs.PublishMsg(ctx, msg)
	}
	appendOp := func(id string) (*jetstream.PubAck, error) {
		msg := nats.NewMsg(contract.OpsSubject("orders", "invoice-1"))
		msg.Header = contract.Op{ID: id, Type: "comment.add", Author: "alice"}.Header()
		msg.Data = []byte(`{"body":"x"}`)
		return mjs.PublishMsg(ctx, msg)
	}

	if _, err := birth("op-1"); err != nil {
		t.Fatalf("birth: %v", err)
	}

	// A retried publish with the same Nats-Msg-Id dedups instead of
	// double-appending.
	if _, err := appendOp("op-2"); err != nil {
		t.Fatalf("append: %v", err)
	}
	ack, err := appendOp("op-2")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !ack.Duplicate {
		t.Fatal("retry with the same op ID must dedup, not append")
	}

	// A second birth of the same thing is refused by the server's own
	// guard — never proxied, never re-implemented.
	if _, err := birth("op-3"); err == nil {
		t.Fatal("second birth landed: the create-if-absent guard is broken")
	} else {
		var apiErr *jetstream.APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Fatalf("expected the server's wrong-last-sequence error, got %v", err)
		}
	}

	// The 0008 build-time verification item, verified: the guard is
	// evaluated before dedup, so a *retried* birth also surfaces
	// wrong-last-sequence — the client disambiguates by reading the
	// subject's last message and comparing op IDs.
	if _, err := birth("op-1"); err == nil {
		t.Fatal("retried birth acked cleanly; the guard-before-dedup premise changed")
	} else {
		var apiErr *jetstream.APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Fatalf("expected wrong-last-sequence on retried birth, got %v", err)
		}
	}
}

// TestTwoInstancesMutateOneTenantWithoutLosingAnUpdate is decision 0030's
// point 3 on a real server: two control instances — two drivers over one
// bucket — revoke different members of one tenant at the same time, many
// rounds. Every revocation must hold on the wire and in custody; the
// compare-and-set makes the loser recompute on top of the winner instead
// of overwriting it.
func TestTwoInstancesMutateOneTenantWithoutLosingAnUpdate(t *testing.T) {
	url, r := natstest.StartSealedOperator(t)
	a := driverOver(t, url, r)
	b := driverOver(t, url, r)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	acct, err := a.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	// Instance B sees the tenant A minted, from custody, at once.
	if got, err := b.Tenant(ctx, "acme"); err != nil || got.PublicKey != acct.PublicKey {
		t.Fatalf("instance B does not see the tenant: %+v, %v", got, err)
	}

	const rounds = 6
	members := make([]mint.Creds, 0, 2*rounds)
	for i := 0; i < 2*rounds; i++ {
		m, err := mint.IssueMember(acct, fmt.Sprintf("m%d", i))
		if err != nil {
			t.Fatalf("issue member: %v", err)
		}
		members = append(members, m)
	}
	for round := 0; round < rounds; round++ {
		x, y := members[2*round], members[2*round+1]
		errs := make(chan error, 2)
		go func() { errs <- a.RevokeUser(ctx, "acme", x.PublicKey) }()
		go func() { errs <- b.RevokeUser(ctx, "acme", y.PublicKey) }()
		for i := 0; i < 2; i++ {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: revoke: %v", round, err)
			}
		}
	}
	// Custody's JWT carries every revocation, and the resolver serves
	// exactly that JWT.
	rec, _, err := a.Custody.Tenant(ctx, "acme")
	if err != nil {
		t.Fatalf("custody: %v", err)
	}
	ac, err := jwt.DecodeAccountClaims(rec.JWT)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, m := range members {
		if !ac.Revocations.IsRevoked(m.PublicKey, time.Now()) {
			t.Fatalf("member %s lost its revocation to a concurrent writer", m.PublicKey)
		}
	}
	if pushed, err := a.Reconcile(ctx); err != nil || len(pushed) != 0 {
		t.Fatalf("resolver disagrees with custody after the race: pushed %v, %v", pushed, err)
	}
	for _, m := range members {
		if nc, err := mint.ConnectCreds(url, m.File, "revoked"); err == nil {
			nc.Close()
			t.Fatalf("revoked member %s still connects", m.PublicKey)
		}
	}
}

// TestReconcilePushesCustodysJWT: when the resolver falls behind custody —
// here by a stale JWT pushed straight to the resolver, the shape of an
// instance that landed its compare-and-set and died before pushing — the
// next Reconcile puts the canonical JWT back.
func TestReconcilePushesCustodysJWT(t *testing.T) {
	url, d := startDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	stale, _, err := d.Custody.Tenant(ctx, "acme")
	if err != nil {
		t.Fatalf("custody: %v", err)
	}
	alice, err := mint.IssueMember(acct, "alice")
	if err != nil {
		t.Fatalf("issue member: %v", err)
	}
	if err := d.RevokeUser(ctx, "acme", alice.PublicKey); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	// The resolver regresses to the pre-revocation JWT.
	if _, err := d.SysConn.RequestWithContext(ctx, "$SYS.REQ.CLAIMS.UPDATE", []byte(stale.JWT)); err != nil {
		t.Fatalf("push stale: %v", err)
	}
	nc, err := mint.ConnectCreds(url, alice.File, "alice-regressed")
	if err != nil {
		t.Fatalf("the regression did not take (alice should connect against the stale JWT): %v", err)
	}
	nc.Close()

	pushed, err := d.Reconcile(ctx)
	if err != nil || len(pushed) != 1 || pushed[0] != "acme" {
		t.Fatalf("reconcile pushed %v, %v", pushed, err)
	}
	if nc, err := mint.ConnectCreds(url, alice.File, "alice-after"); err == nil {
		nc.Close()
		t.Fatal("reconcile did not restore the revocation")
	}
	if pushed, err := d.Reconcile(ctx); err != nil || len(pushed) != 0 {
		t.Fatalf("second reconcile pushed %v, %v", pushed, err)
	}
}
