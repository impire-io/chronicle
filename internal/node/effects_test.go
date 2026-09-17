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

// defineType declares one type with an open thing schema and the given
// operations — one act, all facets (0021).
func defineType(ctx context.Context, t *testing.T, c *client.Client, log, name string, def client.TypeDefinition) client.TypeDefineResponse {
	t.Helper()
	if def.Schema == nil {
		def.Schema = json.RawMessage(`{"type":"object"}`)
	}
	resp, err := c.DefineType(ctx, log, name, def)
	if err != nil {
		t.Fatalf("define type %s: %v", name, err)
	}
	return resp
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
	defineType(ctx, t, alice, "orders", "invoice", client.TypeDefinition{
		Operations: map[string]contract.OpDef{
			"status.set":  {Schema: obj, Effect: contract.EffectMerge},
			"comment.add": {Schema: obj},
		},
	})

	// An effect outside the node's vocabulary is refused at the door.
	if _, err := alice.DefineType(ctx, "orders", "future", client.TypeDefinition{
		Schema:     obj,
		Operations: map[string]contract.OpDef{"future.op": {Schema: obj, Effect: "patch"}},
	}); err == nil {
		t.Fatal("unknown effect accepted")
	} else {
		var serr *client.ServiceError
		if !errors.As(err, &serr) || serr.Code != "bad-effect" {
			t.Fatalf("expected bad-effect, got %v", err)
		}
	}

	birth, err := alice.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"status":"open","assignee":"dana","total":3}`))
	if err != nil {
		t.Fatalf("birth: %v", err)
	}
	waitStateAt(ctx, t, alice, "orders", "invoice.invoice-1", birth.Seq)

	// A merge-effect op moves state: named fields overwrite.
	ack, err := alice.Append(ctx, "orders", "invoice.invoice-1", "status.set", []byte(`{"status":"closed"}`))
	if err != nil {
		t.Fatalf("append status.set: %v", err)
	}
	sv := waitStateAt(ctx, t, alice, "orders", "invoice.invoice-1", ack.Seq)
	var state map[string]any
	if err := json.Unmarshal(sv.State, &state); err != nil {
		t.Fatal(err)
	}
	if state["status"] != "closed" || state["assignee"] != "dana" || state["total"] != float64(3) {
		t.Fatalf("merge lost fields: %s", sv.State)
	}

	// Null deletes, per RFC 7386.
	ack, err = alice.Append(ctx, "orders", "invoice.invoice-1", "status.set", []byte(`{"assignee":null}`))
	if err != nil {
		t.Fatalf("append unassign: %v", err)
	}
	sv = waitStateAt(ctx, t, alice, "orders", "invoice.invoice-1", ack.Seq)
	state = nil
	if err := json.Unmarshal(sv.State, &state); err != nil {
		t.Fatal(err)
	}
	if _, there := state["assignee"]; there {
		t.Fatalf("null must delete: %s", sv.State)
	}
	lastSeq := ack.Seq

	// An effect-none op lives in history and moves nothing.
	if _, err := alice.Append(ctx, "orders", "invoice.invoice-1", "comment.add", []byte(`{"body":"hi"}`)); err != nil {
		t.Fatalf("append comment: %v", err)
	}
	stateStaysAt(ctx, t, alice, "orders", "invoice.invoice-1", lastSeq)

	// An operation the type does not define is refused at pre-flight
	// (0021 § 2) — and an untyped thing keeps publishing freely.
	if _, err := alice.Append(ctx, "orders", "invoice.invoice-1", "no.such", []byte(`{}`)); !errors.Is(err, client.ErrUndefinedOperation) {
		t.Fatalf("undefined operation must refuse at pre-flight: %v", err)
	}
	if _, err := alice.Append(ctx, "orders", "freeform-thing", "no.such", []byte(`{}`)); err != nil {
		t.Fatalf("untyped append must pass pre-flight: %v", err)
	}

	// A schema-invalid op of a merge type is marked and takes no effect.
	raw := nats.NewMsg(contract.OpsSubject("orders", "invoice.invoice-1"))
	raw.Header = contract.Op{ID: "bad-merge", Type: "status.set", Author: "mallory"}.Header()
	raw.Data = []byte(`not json`)
	js := jsFor(t, alice)
	if _, err := js.PublishMsg(ctx, raw); err != nil {
		t.Fatalf("raw publish: %v", err)
	}
	catcher.wait(t, "marked invalid payload")
	stateStaysAt(ctx, t, alice, "orders", "invoice.invoice-1", lastSeq)

	// A merge op on a subject with no snapshot yet takes no effect: the
	// log is malformed for state until one appears.
	if _, err := alice.Append(ctx, "orders", "invoice.unborn-1", "status.set", []byte(`{"status":"x"}`)); err != nil {
		t.Fatalf("append to unborn: %v", err)
	}
	catcher.wait(t, "op before any snapshot takes no effect")
	if _, err := alice.State(ctx, "orders", "invoice.unborn-1"); !errors.Is(err, client.ErrNoState) {
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
	defineType(ctx, t, alice, "orders", "invoice", client.TypeDefinition{
		Operations: map[string]contract.OpDef{"tag.set": {Schema: obj}},
	})

	birth, err := alice.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("birth: %v", err)
	}
	tagged, err := alice.Append(ctx, "orders", "invoice.invoice-1", "tag.set", []byte(`{"tag":"a"}`))
	if err != nil {
		t.Fatalf("append tag: %v", err)
	}
	// Under effect none, the tag moved nothing.
	sv := waitStateAt(ctx, t, alice, "orders", "invoice.invoice-1", birth.Seq)
	if string(sv.State) != `{"n":1}` {
		t.Fatalf("state before change: %s", sv.State)
	}

	// Redefine tag.set as merge: latest declaration wins — the node
	// purges and re-folds, and the whole history is read under the new
	// effect.
	resp, err := alice.DefineType(ctx, "orders", "invoice", client.TypeDefinition{
		Schema:     obj,
		Operations: map[string]contract.OpDef{"tag.set": {Schema: obj, Effect: contract.EffectMerge}},
	})
	if err != nil {
		t.Fatalf("redefine: %v", err)
	}
	if resp.Revision != 2 {
		t.Fatalf("revision = %d", resp.Revision)
	}
	sv = waitStateAt(ctx, t, alice, "orders", "invoice.invoice-1", tagged.Seq)
	var state map[string]any
	if err := json.Unmarshal(sv.State, &state); err != nil {
		t.Fatal(err)
	}
	if state["tag"] != "a" || state["n"] != float64(1) {
		t.Fatalf("rebuilt state: %s", sv.State)
	}

	// And the log itself is untouched — the rebuild was derived-only.
	ops, err := alice.Replay(ctx, "orders", "invoice.invoice-1")
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
