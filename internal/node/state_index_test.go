package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// TestStateIndexDeclaration proves 0023's contract surface: the state
// index's declaration is born with the log, refused to DECLARE (kind and
// reserved name both) and to DELETE, and the fold stamps the bucket with
// the declaration watermark checkpoints bootstrap from.
func TestStateIndexDeclaration(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}

	// The declaration exists because the log does.
	js := jsFor(t, alice)
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		t.Fatalf("open META: %v", err)
	}
	entry, err := meta.Get(ctx, contract.MetaIndex("orders", contract.StateIndexName))
	if err != nil {
		t.Fatalf("state declaration missing: %v", err)
	}
	var decl contract.IndexDeclaration
	if err := json.Unmarshal(entry.Value(), &decl); err != nil || decl.Kind != contract.IndexKindState {
		t.Fatalf("state declaration = %s (%v)", entry.Value(), err)
	}

	// DECLARE refuses the kind, and the reserved name under any kind.
	refused := func(err error) {
		t.Helper()
		var serr *client.ServiceError
		if err == nil || !errors.As(err, &serr) || serr.Code != "reserved-state-index" {
			t.Fatalf("expected reserved-state-index, got %v", err)
		}
	}
	_, err = alice.DeclareIndex(ctx, "orders", "mine", contract.IndexKindState, nil)
	refused(err)
	_, err = alice.DeclareIndex(ctx, "orders", contract.StateIndexName, "search", nil)
	refused(err)

	// DELETE declines it while the log exists.
	_, err = alice.DeleteIndex(ctx, "orders", contract.StateIndexName)
	refused(err)

	// The fold stamped the bucket with the declaration watermark.
	states, err := js.KeyValue(ctx, contract.StateBucket("orders"))
	if err != nil {
		t.Fatalf("open state bucket: %v", err)
	}
	wmEntry, err := states.Get(ctx, contract.StateFoldKey)
	if err != nil {
		t.Fatalf("fold watermark missing: %v", err)
	}
	var wm contract.FoldWatermark
	if err := json.Unmarshal(wmEntry.Value(), &wm); err != nil {
		t.Fatalf("decode watermark: %v", err)
	}

	// A declaration change re-stamps it: the watermark follows the
	// declarations, so stale checkpoints void themselves.
	defineType(ctx, t, alice, "orders", "invoice", client.TypeDefinition{
		Operations: map[string]contract.OpDef{"status.set": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge}},
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		wmEntry, err = states.Get(ctx, contract.StateFoldKey)
		if err == nil {
			var next contract.FoldWatermark
			if json.Unmarshal(wmEntry.Value(), &next) == nil && next.Declarations != wm.Declarations {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("watermark never followed the declaration change: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
