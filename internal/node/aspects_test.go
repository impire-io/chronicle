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
)

// TestAspects proves 0022's contract end to end: an aspect is a typed
// thing under its parent's prefix, folding by its own type's declaration;
// an undeclared aspect is refused at pre-flight, marked at the fold, and
// redeemed retroactively when the declaration arrives; the parent
// compacts while a preserved aspect keeps its trail.
func TestAspects(t *testing.T) {
	catcher := &logCatcher{}
	_, alice := startNode(t, catcher)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "billing", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	obj := json.RawMessage(`{"type":"object"}`)
	defineType(ctx, t, alice, "billing", "invoice", client.TypeDefinition{
		Aspects: map[string]string{"comments": "comment"},
		Operations: map[string]contract.OpDef{
			"status.set": {Schema: obj, Effect: contract.EffectMerge},
		},
	})
	defineType(ctx, t, alice, "billing", "comment", client.TypeDefinition{
		History: contract.HistoryPreserved,
		Aspects: map[string]string{"attachments": "attachment"},
		Operations: map[string]contract.OpDef{
			"edit": {Schema: obj, Effect: contract.EffectMerge},
		},
	})
	defineType(ctx, t, alice, "billing", "attachment", client.TypeDefinition{})

	// The parent and its aspect are independent subjects with independent
	// folds.
	if _, err := alice.CreateThing(ctx, "billing", "invoice.inv-1", json.RawMessage(`{"status":"open"}`)); err != nil {
		t.Fatalf("birth invoice: %v", err)
	}
	if _, err := alice.Append(ctx, "billing", "invoice.inv-1", "status.set", []byte(`{"status":"closed"}`)); err != nil {
		t.Fatalf("append status: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "billing", "invoice.inv-1.comments.c-1", json.RawMessage(`{"body":"first"}`)); err != nil {
		t.Fatalf("birth comment: %v", err)
	}
	edited, err := alice.Append(ctx, "billing", "invoice.inv-1.comments.c-1", "edit", []byte(`{"body":"edited"}`))
	if err != nil {
		t.Fatalf("append edit: %v", err)
	}
	sv := waitStateAt(ctx, t, alice, "billing", "invoice.inv-1.comments.c-1", edited.Seq)
	if !strings.Contains(string(sv.State), "edited") {
		t.Fatalf("aspect state did not fold: %s", sv.State)
	}

	// Multi-level: an aspect of an aspect resolves through the chain.
	if _, err := alice.CreateThing(ctx, "billing", "invoice.inv-1.comments.c-1.attachments.a-1", json.RawMessage(`{"name":"scan.pdf"}`)); err != nil {
		t.Fatalf("birth nested aspect: %v", err)
	}

	// The parent compacts; the preserved aspect declines with its type
	// named — per-subject roll-up, each by its own declaration.
	res, err := alice.RollupThing(ctx, "billing", "invoice.inv-1")
	if err != nil {
		t.Fatalf("rollup parent: %v", err)
	}
	if !res.Rolled {
		t.Fatalf("the parent must compact: %+v", res)
	}
	res, err = alice.RollupThing(ctx, "billing", "invoice.inv-1.comments.c-1")
	if err != nil {
		t.Fatalf("rollup aspect: %v", err)
	}
	if res.Rolled || !strings.Contains(res.Reason, "preserved") {
		t.Fatalf("a preserved aspect must decline by declaration: %+v", res)
	}
	if ops := replayOf(ctx, t, alice, "billing", "invoice.inv-1.comments.c-1"); len(ops) != 2 {
		t.Fatalf("the aspect's trail must stand whole: %d ops", len(ops))
	}

	// An undeclared aspect is refused at pre-flight (0022 § 3)…
	if _, err := alice.CreateThing(ctx, "billing", "invoice.inv-1.attachments.a-9", json.RawMessage(`{}`)); !errors.Is(err, client.ErrUndeclaredAspect) {
		t.Fatalf("undeclared aspect must refuse at pre-flight: %v", err)
	}
	// …and a raw publish past the SDK is marked at the fold: in the log,
	// out of derived state.
	payload, _ := json.Marshal(contract.Snapshot{State: json.RawMessage(`{"n":1}`), Frontier: []string{}})
	raw := nats.NewMsg(contract.OpsSubject("billing", "invoice.inv-1.attachments.a-9"))
	raw.Header = contract.Op{ID: "sneak-1", Type: contract.OpTypeSnapshot, Author: "mallory"}.Header()
	raw.Header.Set(contract.HdrExpectedLastSubjSeq, "0")
	raw.Data = payload
	if _, err := jsFor(t, alice).PublishMsg(ctx, raw); err != nil {
		t.Fatalf("raw publish: %v", err)
	}
	catcher.wait(t, "marked undeclared aspect")
	if _, err := alice.State(ctx, "billing", "invoice.inv-1.attachments.a-9"); !errors.Is(err, client.ErrNoState) {
		t.Fatalf("a marked subject grew state: %v", err)
	}
	if ops := replayOf(ctx, t, alice, "billing", "invoice.inv-1.attachments.a-9"); len(ops) != 1 {
		t.Fatalf("the log must keep the marked birth: %d ops", len(ops))
	}

	// Latest declaration wins: declaring the segment redeems the marked
	// subject retroactively on the rebuild.
	if _, err := alice.DefineType(ctx, "billing", "invoice", client.TypeDefinition{
		Schema:  obj,
		Aspects: map[string]string{"comments": "comment", "attachments": "attachment"},
		Operations: map[string]contract.OpDef{
			"status.set": {Schema: obj, Effect: contract.EffectMerge},
		},
	}); err != nil {
		t.Fatalf("redefine parent: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		sv, err := alice.State(ctx, "billing", "invoice.inv-1.attachments.a-9")
		if err == nil && strings.Contains(string(sv.State), `"n":1`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the declaration never redeemed the marked subject: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
