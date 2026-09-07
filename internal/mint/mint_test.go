package mint_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/natstest"
)

func startDriver(t *testing.T) (url string, d *mint.JWTDriver) {
	t.Helper()
	url, b := natstest.StartOperator(t)
	sysConn, err := mint.ConnectCreds(url, b.SysCreds, "test-sys")
	if err != nil {
		t.Fatalf("connect system user: %v", err)
	}
	t.Cleanup(sysConn.Close)
	return url, &mint.JWTDriver{
		OperatorSigningSeed: b.OperatorSigningSeed,
		SysConn:             sysConn,
		URL:                 url,
	}
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
	if _, err := js.CreateStream(ctx, contract.LogStreamConfig("orders", 0)); err != nil {
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
	if _, err := sjs.CreateStream(ctx, contract.LogStreamConfig("orders", 0)); err != nil {
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
	if _, err := mjs.CreateStream(shortCtx2, contract.LogStreamConfig("rogue", 0)); err == nil {
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
	if _, err := sjs.CreateStream(ctx, contract.LogStreamConfig("orders", 0)); err != nil {
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
