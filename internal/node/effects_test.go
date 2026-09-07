package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// waitStateAt polls until the thing's state reaches wantSeq.
func waitStateAt(ctx context.Context, t *testing.T, c *client.Client, log, thing string, wantSeq uint64) contract.StateValue {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		sv, err := c.State(ctx, log, thing)
		if err == nil && sv.Seq == wantSeq {
			return sv
		}
		if time.Now().After(deadline) {
			t.Fatalf("state never reached seq %d: %+v %v", wantSeq, sv, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// stateStaysAt asserts the thing's state has not moved past haveSeq.
func stateStaysAt(ctx context.Context, t *testing.T, c *client.Client, log, thing string, haveSeq uint64) {
	t.Helper()
	time.Sleep(300 * time.Millisecond) // give a wrong fold time to be wrong
	sv, err := c.State(ctx, log, thing)
	if err != nil {
		t.Fatalf("state read: %v", err)
	}
	if sv.Seq != haveSeq {
		t.Fatalf("state moved to seq %d; must stay at %d", sv.Seq, haveSeq)
	}
}

func TestMergeEffect(t *testing.T) {
	catcher := &logCatcher{}
	_, alice := startNode(t, catcher)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	obj := json.RawMessage(`{"type":"object"}`)
	if _, err := alice.SetSchema(ctx, "orders", "status.set", obj, contract.EffectMerge); err != nil {
		t.Fatalf("declare status.set: %v", err)
	}
	if _, err := alice.SetSchema(ctx, "orders", "comment.add", obj, ""); err != nil {
		t.Fatalf("declare comment.add: %v", err)
	}

	// An effect outside the node's vocabulary is refused at the door.
	if _, err := alice.SetSchema(ctx, "orders", "future.op", obj, "patch"); err == nil {
		t.Fatal("unknown effect accepted")
	} else {
		var serr *client.ServiceError
		if !errors.As(err, &serr) || serr.Code != "bad-effect" {
			t.Fatalf("expected bad-effect, got %v", err)
		}
	}

	birth, err := alice.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"status":"open","assignee":"dana","total":3}`))
	if err != nil {
		t.Fatalf("birth: %v", err)
	}
	waitStateAt(ctx, t, alice, "orders", "invoice-1", birth.Seq)

	// A merge-effect op moves state: named fields overwrite.
	ack, err := alice.Append(ctx, "orders", "invoice-1", "status.set", []byte(`{"status":"closed"}`))
	if err != nil {
		t.Fatalf("append status.set: %v", err)
	}
	sv := waitStateAt(ctx, t, alice, "orders", "invoice-1", ack.Seq)
	var state map[string]any
	if err := json.Unmarshal(sv.State, &state); err != nil {
		t.Fatal(err)
	}
	if state["status"] != "closed" || state["assignee"] != "dana" || state["total"] != float64(3) {
		t.Fatalf("merge lost fields: %s", sv.State)
	}

	// Null deletes, per RFC 7386.
	ack, err = alice.Append(ctx, "orders", "invoice-1", "status.set", []byte(`{"assignee":null}`))
	if err != nil {
		t.Fatalf("append unassign: %v", err)
	}
	sv = waitStateAt(ctx, t, alice, "orders", "invoice-1", ack.Seq)
	state = nil
	if err := json.Unmarshal(sv.State, &state); err != nil {
		t.Fatal(err)
	}
	if _, there := state["assignee"]; there {
		t.Fatalf("null must delete: %s", sv.State)
	}
	lastSeq := ack.Seq

	// An effect-none op lives in history and moves nothing.
	if _, err := alice.Append(ctx, "orders", "invoice-1", "comment.add", []byte(`{"body":"hi"}`)); err != nil {
		t.Fatalf("append comment: %v", err)
	}
	stateStaysAt(ctx, t, alice, "orders", "invoice-1", lastSeq)

	// A schema-invalid op of a merge type is marked and takes no effect.
	raw := nats.NewMsg(contract.OpsSubject("orders", "invoice-1"))
	raw.Header = contract.Op{ID: "bad-merge", Type: "status.set", Author: "mallory"}.Header()
	raw.Data = []byte(`not json`)
	js := jsFor(t, alice)
	if _, err := js.PublishMsg(ctx, raw); err != nil {
		t.Fatalf("raw publish: %v", err)
	}
	catcher.wait(t, "marked invalid payload")
	stateStaysAt(ctx, t, alice, "orders", "invoice-1", lastSeq)

	// A merge op on a subject with no snapshot yet takes no effect: the
	// log is malformed for state until one appears.
	if _, err := alice.Append(ctx, "orders", "unborn-1", "status.set", []byte(`{"status":"x"}`)); err != nil {
		t.Fatalf("append to unborn: %v", err)
	}
	catcher.wait(t, "op before any snapshot takes no effect")
	if _, err := alice.State(ctx, "orders", "unborn-1"); !errors.Is(err, client.ErrNoState) {
		t.Fatalf("unborn thing grew state: %v", err)
	}
}

func TestEffectChangeRebuildsState(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	obj := json.RawMessage(`{"type":"object"}`)
	if _, err := alice.SetSchema(ctx, "orders", "tag.set", obj, ""); err != nil {
		t.Fatalf("declare tag.set: %v", err)
	}

	birth, err := alice.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("birth: %v", err)
	}
	tagged, err := alice.Append(ctx, "orders", "invoice-1", "tag.set", []byte(`{"tag":"a"}`))
	if err != nil {
		t.Fatalf("append tag: %v", err)
	}
	// Under effect none, the tag moved nothing.
	sv := waitStateAt(ctx, t, alice, "orders", "invoice-1", birth.Seq)
	if string(sv.State) != `{"n":1}` {
		t.Fatalf("state before change: %s", sv.State)
	}

	// Redeclare tag.set as merge: latest declaration wins — the node
	// purges and re-folds, and the whole history is read under the new
	// effect.
	resp, err := alice.SetSchema(ctx, "orders", "tag.set", obj, contract.EffectMerge)
	if err != nil {
		t.Fatalf("redeclare: %v", err)
	}
	if resp.Revision != 2 {
		t.Fatalf("revision = %d", resp.Revision)
	}
	sv = waitStateAt(ctx, t, alice, "orders", "invoice-1", tagged.Seq)
	var state map[string]any
	if err := json.Unmarshal(sv.State, &state); err != nil {
		t.Fatal(err)
	}
	if state["tag"] != "a" || state["n"] != float64(1) {
		t.Fatalf("rebuilt state: %s", sv.State)
	}

	// And the log itself is untouched — the rebuild was derived-only.
	ops, err := alice.Replay(ctx, "orders", "invoice-1")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("replay after rebuild: %d ops", len(ops))
	}
}

// jsFor opens a JetStream handle on the client's own connection for
// raw-wire publishes.
func jsFor(t *testing.T, c *client.Client) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(c.Conn())
	if err != nil {
		t.Fatal(err)
	}
	return js
}
