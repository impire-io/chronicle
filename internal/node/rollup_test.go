package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/node"
)

// replayOf reads a thing's history, failing the test on error.
func replayOf(ctx context.Context, t *testing.T, c *client.Client, log, thing string) []contract.Op {
	t.Helper()
	ops, err := c.Replay(ctx, log, thing)
	if err != nil {
		t.Fatalf("replay %s: %v", thing, err)
	}
	return ops
}

func TestSaveVersion(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"total":1}`)); err != nil {
		t.Fatalf("birth: %v", err)
	}
	// An undeclared type: the fold cannot capture it, so the node's own
	// triggers would refuse this history — saving a version is exactly
	// the application's call the gate defers to.
	c1, err := alice.Append(ctx, "orders", "invoice-1", "comment.add", []byte(`{"body":"hi"}`))
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	saved, err := alice.SaveVersion(ctx, "orders", "invoice-1", json.RawMessage(`{"total":1,"comments":1}`), []string{c1.OpID}, c1.Seq)
	if err != nil {
		t.Fatalf("save version: %v", err)
	}
	ops := replayOf(ctx, t, alice, "orders", "invoice-1")
	if len(ops) != 1 || ops[0].Type != contract.OpTypeSnapshot || ops[0].Seq != saved.Seq {
		t.Fatalf("history must be the one saved snapshot: %+v", ops)
	}
	snap, err := contract.ParseSnapshot(ops[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.State) != `{"total":1,"comments":1}` {
		t.Fatalf("saved state: %s", snap.State)
	}
	if len(snap.Frontier) != 1 || snap.Frontier[0] != c1.OpID {
		t.Fatalf("saved frontier: %v", snap.Frontier)
	}
	// The fold converges the bucket onto the saved version.
	sv := waitStateAt(ctx, t, alice, "orders", "invoice-1", saved.Seq)
	if string(sv.State) != `{"total":1,"comments":1}` {
		t.Fatalf("bucket state: %s", sv.State)
	}

	// A save whose upTo the log moved past is refused; nothing changes.
	c2, err := alice.Append(ctx, "orders", "invoice-1", "comment.add", []byte(`{"body":"again"}`))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := alice.SaveVersion(ctx, "orders", "invoice-1", json.RawMessage(`{}`), nil, saved.Seq); !errors.Is(err, client.ErrStaleVersion) {
		t.Fatalf("expected ErrStaleVersion, got %v", err)
	}
	if ops := replayOf(ctx, t, alice, "orders", "invoice-1"); len(ops) != 2 {
		t.Fatalf("a stale save must not touch the log: %d ops", len(ops))
	}

	// A retried save whose op already landed reports the original landing.
	v2, err := alice.SaveVersion(ctx, "orders", "invoice-1", json.RawMessage(`{"total":1,"comments":2}`), []string{c2.OpID}, c2.Seq, client.WithOpID("save-v2"))
	if err != nil {
		t.Fatalf("save v2: %v", err)
	}
	again, err := alice.SaveVersion(ctx, "orders", "invoice-1", json.RawMessage(`{"total":1,"comments":2}`), []string{c2.OpID}, c2.Seq, client.WithOpID("save-v2"))
	if err != nil {
		t.Fatalf("save v2 retry: %v", err)
	}
	if again.Seq != v2.Seq {
		t.Fatalf("retry landed elsewhere: %d vs %d", again.Seq, v2.Seq)
	}

	// Birth is CreateThing, not a save at upTo zero.
	if _, err := alice.SaveVersion(ctx, "orders", "invoice-2", json.RawMessage(`{}`), nil, 0); err == nil {
		t.Fatal("save at upTo 0 accepted")
	}
}

func TestThingRollupVerb(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	// A fully captured history compacts to one snapshot.
	if _, err := alice.CreateThing(ctx, "orders", "invoice.inv-1", json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatalf("birth: %v", err)
	}
	if _, err := alice.Append(ctx, "orders", "invoice.inv-1", "status.set", []byte(`{"b":2}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	last, err := alice.Append(ctx, "orders", "invoice.inv-1", "status.set", []byte(`{"a":3}`))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	res, err := alice.RollupThing(ctx, "orders", "invoice.inv-1")
	if err != nil {
		t.Fatalf("rollup: %v", err)
	}
	if !res.Rolled || res.Seq <= last.Seq {
		t.Fatalf("rollup declined: %+v", res)
	}
	ops := replayOf(ctx, t, alice, "orders", "invoice.inv-1")
	if len(ops) != 1 || ops[0].Type != contract.OpTypeSnapshot {
		t.Fatalf("history must be the rollup snapshot: %+v", ops)
	}
	snap, err := contract.ParseSnapshot(ops[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal(snap.State, &state); err != nil {
		t.Fatal(err)
	}
	if state["a"] != float64(3) || state["b"] != float64(2) {
		t.Fatalf("rolled state: %s", snap.State)
	}
	if len(snap.Frontier) != 1 || snap.Frontier[0] != last.OpID {
		t.Fatalf("rolled frontier: %v", snap.Frontier)
	}
	sv := waitStateAt(ctx, t, alice, "orders", "invoice.inv-1", res.Seq)
	state = nil
	if err := json.Unmarshal(sv.State, &state); err != nil {
		t.Fatal(err)
	}
	if state["a"] != float64(3) || state["b"] != float64(2) {
		t.Fatalf("bucket state after rollup: %s", sv.State)
	}
	// The subject looks like it did at birth: snapshot first, ops after.
	after, err := alice.Append(ctx, "orders", "invoice.inv-1", "status.set", []byte(`{"c":4}`))
	if err != nil {
		t.Fatalf("append after rollup: %v", err)
	}
	waitStateAt(ctx, t, alice, "orders", "invoice.inv-1", after.Seq)

	// On a typed, compactable thing the per-op veto is retired (0022 § 5):
	// an effect-none op is absorbed by declaration — the type author said
	// this history is absorbable, and ops that must survive belong on a
	// preserved aspect.
	if _, err := alice.CreateThing(ctx, "orders", "invoice.inv-2", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatalf("birth: %v", err)
	}
	if _, err := alice.Append(ctx, "orders", "invoice.inv-2", "comment.add", []byte(`{"body":"hi"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	res, err = alice.RollupThing(ctx, "orders", "invoice.inv-2")
	if err != nil {
		t.Fatalf("rollup: %v", err)
	}
	if !res.Rolled {
		t.Fatalf("a typed compactable thing must compact past a none op: %+v", res)
	}
	ops = replayOf(ctx, t, alice, "orders", "invoice.inv-2")
	if len(ops) != 1 || ops[0].Type != contract.OpTypeSnapshot {
		t.Fatalf("the none op must be absorbed: %+v", ops)
	}
	if snap, err := contract.ParseSnapshot(ops[0].Payload); err != nil || string(snap.State) != `{"n":1}` {
		t.Fatalf("absorbed state: %v %s", err, snap.State)
	}

	// Marked junk on a typed thing is absorbed the same way: the rollup
	// snapshot keeps exactly what folded state keeps.
	if _, err := alice.CreateThing(ctx, "orders", "invoice.inv-4", json.RawMessage(`{"k":1}`)); err != nil {
		t.Fatalf("birth: %v", err)
	}
	raw := nats.NewMsg(contract.OpsSubject("orders", "invoice.inv-4"))
	raw.Header = contract.Op{ID: "bad-1", Type: "status.set", Author: "mallory"}.Header()
	raw.Data = []byte(`not json`)
	if _, err := jsFor(t, alice).PublishMsg(ctx, raw); err != nil {
		t.Fatalf("raw publish: %v", err)
	}
	res, err = alice.RollupThing(ctx, "orders", "invoice.inv-4")
	if err != nil {
		t.Fatalf("rollup: %v", err)
	}
	if !res.Rolled {
		t.Fatalf("a typed compactable thing must compact past marked junk: %+v", res)
	}

	// Where no type resolves, 0011 § 4 stands whole: what the fold could
	// not capture, the node must not destroy.
	if _, err := alice.CreateThing(ctx, "orders", "freeform-1", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatalf("birth: %v", err)
	}
	if _, err := alice.Append(ctx, "orders", "freeform-1", "mystery.op", []byte(`{}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	res, err = alice.RollupThing(ctx, "orders", "freeform-1")
	if err != nil {
		t.Fatalf("rollup: %v", err)
	}
	if res.Rolled || !strings.Contains(res.Reason, "untyped") {
		t.Fatalf("an untyped thing's op must veto: %+v", res)
	}
	if ops := replayOf(ctx, t, alice, "orders", "freeform-1"); len(ops) != 2 {
		t.Fatalf("a veto must not touch the log: %d ops", len(ops))
	}

	// A bare birth has nothing for a rollup to destroy.
	if _, err := alice.CreateThing(ctx, "orders", "invoice.inv-5", nil); err != nil {
		t.Fatalf("birth: %v", err)
	}
	res, err = alice.RollupThing(ctx, "orders", "invoice.inv-5")
	if err != nil {
		t.Fatalf("rollup: %v", err)
	}
	if res.Rolled || res.Reason != "nothing to compact" {
		t.Fatalf("a bare birth must skip: %+v", res)
	}

	// Writers may trigger; readers are refused.
	wally, err := client.Wrap(alice.Conn(), "wally")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wally.RollupThing(ctx, "orders", "invoice.inv-5"); err != nil {
		t.Fatalf("a writer must be allowed to trigger: %v", err)
	}
	rita, err := client.Wrap(alice.Conn(), "rita")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rita.RollupThing(ctx, "orders", "invoice.inv-5"); err == nil {
		t.Fatal("a reader triggered a rollup")
	} else {
		var serr *client.ServiceError
		if !errors.As(err, &serr) || serr.Code != "forbidden" {
			t.Fatalf("expected forbidden, got %v", err)
		}
	}

	// Unknown log and unknown thing are named refusals.
	if _, err := alice.RollupThing(ctx, "nolog", "x"); err == nil {
		t.Fatal("unknown log accepted")
	} else {
		var serr *client.ServiceError
		if !errors.As(err, &serr) || serr.Code != "no-such-log" {
			t.Fatalf("expected no-such-log, got %v", err)
		}
	}
	if _, err := alice.RollupThing(ctx, "orders", "ghost"); err == nil {
		t.Fatal("unknown thing accepted")
	} else {
		var serr *client.ServiceError
		if !errors.As(err, &serr) || serr.Code != "no-such-thing" {
			t.Fatalf("expected no-such-thing, got %v", err)
		}
	}
}

func TestRollupTimer(t *testing.T) {
	_, alice := startNodeWith(t, nil, node.Config{RollupEvery: 150 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	if _, err := alice.CreateThing(ctx, "orders", "invoice.inv-1", json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatalf("birth: %v", err)
	}
	if _, err := alice.Append(ctx, "orders", "invoice.inv-1", "status.set", []byte(`{"b":2}`)); err != nil {
		t.Fatalf("append: %v", err)
	}

	// The timer sweeps the active subject and compacts it — no verb call.
	deadline := time.Now().Add(15 * time.Second)
	for {
		ops := replayOf(ctx, t, alice, "orders", "invoice.inv-1")
		if len(ops) == 1 && ops[0].Type == contract.OpTypeSnapshot {
			snap, err := contract.ParseSnapshot(ops[0].Payload)
			if err != nil {
				t.Fatal(err)
			}
			var state map[string]any
			if err := json.Unmarshal(snap.State, &state); err != nil {
				t.Fatal(err)
			}
			if state["a"] != float64(1) || state["b"] != float64(2) {
				t.Fatalf("timer rolled the wrong state: %s", snap.State)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the timer never compacted: %d ops", len(ops))
		}
		time.Sleep(50 * time.Millisecond)
	}

	// A vetoed subject stays whole across sweeps — untyped, so 0011 § 4's
	// veto still governs it.
	if _, err := alice.CreateThing(ctx, "orders", "freeform-2", nil); err != nil {
		t.Fatalf("birth: %v", err)
	}
	if _, err := alice.Append(ctx, "orders", "freeform-2", "note.add", []byte(`{"body":"x"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	time.Sleep(600 * time.Millisecond) // several sweeps
	if ops := replayOf(ctx, t, alice, "orders", "freeform-2"); len(ops) != 2 {
		t.Fatalf("the timer compacted a vetoed subject: %d ops", len(ops))
	}
}
