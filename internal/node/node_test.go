package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/internal/node"
)

// logCatcher collects the fold's structured warnings, so the tests can
// assert warn-and-continue and mark-never-drop.
type logCatcher struct {
	mu    sync.Mutex
	lines []string
}

func (l *logCatcher) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(l, nil))
}

func (l *logCatcher) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, string(p))
	return len(p), nil
}

func (l *logCatcher) wait(t *testing.T, substr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		for _, line := range l.lines {
			if strings.Contains(line, substr) {
				l.mu.Unlock()
				return
			}
		}
		l.mu.Unlock()
		time.Sleep(25 * time.Millisecond)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	t.Fatalf("no log line containing %q; got %v", substr, l.lines)
}

// startNode provisions META the way control does — bucket plus identity
// slice — and starts a node over a plain JetStream server: the in-account
// wire behavior is account-independent.
func startNode(t *testing.T, catcher *logCatcher) (nc *nats.Conn, alice *client.Client) {
	t.Helper()
	return startNodeWith(t, catcher, node.Config{})
}

// startNodeWith is startNode with the node's Config in the caller's hands
// (the rollup timer tests shrink RollupEvery).
func startNodeWith(t *testing.T, catcher *logCatcher, cfg node.Config) (nc *nats.Conn, alice *client.Client) {
	t.Helper()
	url := natstest.StartJetStream(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	meta, err := js.CreateKeyValue(ctx, contract.MetaBucketConfig())
	if err != nil {
		t.Fatalf("create META: %v", err)
	}
	principal, _ := json.Marshal(contract.Principal{ID: "alice", Name: "Alice"})
	if _, err := meta.Put(ctx, contract.MetaPrincipal("alice"), principal); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
	member, _ := json.Marshal(contract.Membership{PublicKey: "UTEST", Role: contract.RoleAdmin})
	if _, err := meta.Put(ctx, contract.MetaMember("alice"), member); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	reader, _ := json.Marshal(contract.Membership{PublicKey: "UTEST2", Role: contract.RoleReader})
	if _, err := meta.Put(ctx, contract.MetaMember("rita"), reader); err != nil {
		t.Fatalf("seed reader: %v", err)
	}
	writer, _ := json.Marshal(contract.Membership{PublicKey: "UTEST3", Role: contract.RoleWriter})
	if _, err := meta.Put(ctx, contract.MetaMember("wally"), writer); err != nil {
		t.Fatalf("seed writer: %v", err)
	}

	if catcher != nil {
		cfg.Logger = catcher.logger()
	}
	n, err := node.Start(ctx, nc, cfg)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(n.Stop)

	cnc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(cnc.Close)
	alice, err = client.Wrap(cnc, "alice")
	if err != nil {
		t.Fatalf("wrap client: %v", err)
	}
	return nc, alice
}

func TestCreateLogAndSpine(t *testing.T) {
	catcher := &logCatcher{}
	_, alice := startNode(t, catcher)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// FR-02: create a log — stream plus META entries.
	created, err := alice.CreateLog(ctx, "orders", "the orders log")
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	if created.Stream != "LOG_ORDERS" {
		t.Fatalf("stream name %q", created.Stream)
	}

	// Creating it again is refused: the META key is the claim.
	if _, err := alice.CreateLog(ctx, "orders", ""); err == nil {
		t.Fatal("duplicate create landed")
	} else {
		var serr *client.ServiceError
		if !errors.As(err, &serr) || serr.Code != "log-exists" {
			t.Fatalf("expected log-exists, got %v", err)
		}
	}
	// Reserved and malformed names are refused.
	for _, bad := range []string{"api", "Orders", "my_log"} {
		if _, err := alice.CreateLog(ctx, bad, ""); err == nil {
			t.Fatalf("bad log name %q accepted", bad)
		}
	}

	// Schemas: declared, revisioned, compiled before recorded.
	schema := json.RawMessage(`{"type":"object","required":["body"],"properties":{"body":{"type":"string"}}}`)
	rev, err := alice.SetSchema(ctx, "orders", "comment.add", schema, "")
	if err != nil {
		t.Fatalf("set schema: %v", err)
	}
	if rev.Revision != 1 {
		t.Fatalf("first revision = %d", rev.Revision)
	}
	rev2, err := alice.SetSchema(ctx, "orders", "comment.add", schema, "")
	if err != nil {
		t.Fatalf("set schema again: %v", err)
	}
	if rev2.Revision != 2 {
		t.Fatalf("second revision = %d", rev2.Revision)
	}
	if _, err := alice.SetSchema(ctx, "orders", "broken", json.RawMessage(`{"type":"nope"}`), ""); err == nil {
		t.Fatal("uncompilable schema recorded")
	}

	// FR-03: birth, then appends with pre-flight.
	birth, err := alice.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"total":0}`))
	if err != nil {
		t.Fatalf("create thing: %v", err)
	}
	// Idempotent birth retry: same op ID reports the same landing.
	again, err := alice.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"total":0}`), client.WithOpID(birth.OpID))
	if err != nil {
		t.Fatalf("birth retry: %v", err)
	}
	if again.Seq != birth.Seq {
		t.Fatalf("birth retry landed elsewhere: %d vs %d", again.Seq, birth.Seq)
	}
	// A different writer's birth of the same thing is refused.
	if _, err := alice.CreateThing(ctx, "orders", "invoice-1", nil); !errors.Is(err, client.ErrThingExists) {
		t.Fatalf("expected ErrThingExists, got %v", err)
	}

	// Pre-flight refuses an invalid payload before the wire.
	if _, err := alice.Append(ctx, "orders", "invoice-1", "comment.add", []byte(`{"nobody":1}`)); !errors.Is(err, client.ErrSchemaViolation) {
		t.Fatalf("expected schema violation, got %v", err)
	}
	// A valid op lands.
	ack, err := alice.Append(ctx, "orders", "invoice-1", "comment.add", []byte(`{"body":"first"}`), client.WithParents(birth.OpID))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if ack.Seq <= birth.Seq {
		t.Fatalf("append seq %d not after birth %d", ack.Seq, birth.Seq)
	}

	// FR-04: the fold wrote the birth snapshot into the state bucket.
	waitState := func(wantSeq uint64) contract.StateValue {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			sv, err := alice.State(ctx, "orders", "invoice-1")
			if err == nil && sv.Seq == wantSeq {
				return sv
			}
			if time.Now().After(deadline) {
				t.Fatalf("state never reached seq %d: %+v %v", wantSeq, sv, err)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	sv := waitState(birth.Seq)
	if string(sv.State) != `{"total":0}` {
		t.Fatalf("state %s", sv.State)
	}

	// A later snapshot advances the state; the pattern's floor.
	snap2, err := json.Marshal(contract.Snapshot{State: json.RawMessage(`{"total":7}`), Frontier: []string{ack.OpID}})
	if err != nil {
		t.Fatal(err)
	}
	snapAck, err := alice.Append(ctx, "orders", "invoice-1", contract.OpTypeSnapshot, snap2)
	if err != nil {
		t.Fatalf("snapshot append: %v", err)
	}
	sv = waitState(snapAck.Seq)
	if string(sv.State) != `{"total":7}` {
		t.Fatalf("state after snapshot %s", sv.State)
	}

	// FR-05: replay returns the full history in stream order; FoldTail
	// picks up exactly after the state's seq.
	ops, err := alice.Replay(ctx, "orders", "invoice-1")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(ops) != 3 {
		t.Fatalf("replay returned %d ops", len(ops))
	}
	if ops[0].Type != contract.OpTypeSnapshot || ops[0].Seq != birth.Seq {
		t.Fatalf("replay must start at the birth snapshot: %+v", ops[0])
	}
	if ops[1].Author != "alice" || ops[1].Parents[0] != birth.OpID {
		t.Fatalf("the record lost author or parents: %+v", ops[1])
	}
	var tail []contract.Op
	if err := alice.FoldTail(ctx, "orders", "invoice-1", sv.Seq, func(op contract.Op) error {
		tail = append(tail, op)
		return nil
	}); err != nil {
		t.Fatalf("fold tail: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("tail after the latest snapshot must be empty, got %d", len(tail))
	}

	// Tolerance: unknown op types warn; invalid payloads of known types
	// are marked; neither drops, and the log keeps both.
	if _, err := alice.Append(ctx, "orders", "invoice-1", "mystery.op", []byte(`{}`)); err != nil {
		t.Fatalf("unknown type must publish: %v", err)
	}
	catcher.wait(t, "unknown op type ignored")
	raw := nats.NewMsg(contract.OpsSubject("orders", "invoice-1"))
	raw.Header = contract.Op{ID: "bad-1", Type: "comment.add", Author: "mallory"}.Header()
	raw.Data = []byte(`{"nobody":1}`)
	js, err := jetstream.New(aliceConn(alice))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.PublishMsg(ctx, raw); err != nil {
		t.Fatalf("raw invalid publish: %v", err)
	}
	catcher.wait(t, "marked invalid payload")
	ops, err = alice.Replay(ctx, "orders", "invoice-1")
	if err != nil {
		t.Fatalf("replay after junk: %v", err)
	}
	if len(ops) != 5 {
		t.Fatalf("the log must keep junk, warts included: %d ops", len(ops))
	}

	// Roles gate the control verbs: a reader principal cannot create logs.
	ritaClient, err := client.Wrap(aliceConn(alice), "rita")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ritaClient.CreateLog(ctx, "rogue", ""); err == nil {
		t.Fatal("reader created a log")
	} else {
		var serr *client.ServiceError
		if !errors.As(err, &serr) || serr.Code != "forbidden" {
			t.Fatalf("expected forbidden, got %v", err)
		}
	}
}

// aliceConn digs the raw connection back out for raw-wire publishes.
func aliceConn(c *client.Client) *nats.Conn { return c.Conn() }

func TestNodeRestartsFoldsFromMeta(t *testing.T) {
	nc, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	birth, err := alice.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("create thing: %v", err)
	}

	// A second node (a restart's shape) reads the inventory from META and
	// folds the same log; the CAS keeps double-folding harmless.
	n2, err := node.Start(ctx, nc, node.Config{})
	if err != nil {
		t.Fatalf("second node: %v", err)
	}
	defer n2.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for {
		sv, err := alice.State(ctx, "orders", "invoice-1")
		if err == nil && sv.Seq == birth.Seq {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state not folded after restart: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
