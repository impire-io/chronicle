package node_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/node"
)

// TestPreservedHistory proves 0019's contract: a log declared
// history-preserved is never compacted by the node, and its stream
// refuses rollup writes outright — regardless of effect coverage.
func TestPreservedHistory(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Write-side strict: a value outside the vocabulary is refused.
	if _, err := alice.CreateLog(ctx, "audit", "", client.WithHistory("forever")); err == nil {
		t.Fatal("unknown history value accepted")
	}

	if _, err := alice.CreateLog(ctx, "audit", "the audit trail", client.WithHistory(contract.HistoryPreserved)); err != nil {
		t.Fatalf("create preserved log: %v", err)
	}
	// A fully merge-covered thing — sweep-eligible on any compactable log.
	if _, err := alice.SetSchema(ctx, "audit", "status.set", json.RawMessage(`{"type":"object"}`), contract.EffectMerge); err != nil {
		t.Fatalf("set schema: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "audit", "case-1", json.RawMessage(`{"n":0}`)); err != nil {
		t.Fatalf("birth: %v", err)
	}
	last, err := alice.Append(ctx, "audit", "case-1", "status.set", []byte(`{"status":"open"}`))
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// The node declines the whole log, with the declaration named.
	res, err := alice.RollupThing(ctx, "audit", "case-1")
	if err != nil {
		t.Fatalf("rollup verb: %v", err)
	}
	if res.Rolled || !strings.Contains(res.Reason, "preserved") {
		t.Fatalf("preserved log compacted, or reason unnamed: %+v", res)
	}

	// The guarantee is the server's, not just the node's manners: a
	// rollup write — SaveVersion — is refused by the stream itself.
	if _, err := alice.SaveVersion(ctx, "audit", "case-1", json.RawMessage(`{"n":1}`), nil, last.Seq); err == nil {
		t.Fatal("save version landed on a preserved log")
	}

	// Nothing above touched the trail: birth plus the op, intact.
	ops := replayOf(ctx, t, alice, "audit", "case-1")
	if len(ops) != 2 {
		t.Fatalf("history disturbed: %d ops", len(ops))
	}
}

// TestPreservedLogSurvivesTheSweep runs the timer against two logs of the
// same shape — one compactable, one preserved. The control log compacting
// proves the sweep ran; the preserved log's trail must be intact after it.
func TestPreservedLogSurvivesTheSweep(t *testing.T) {
	_, alice := startNodeWith(t, nil, node.Config{RollupEvery: 50 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seed := func(log string, opts ...client.LogOpt) {
		t.Helper()
		if _, err := alice.CreateLog(ctx, log, "", opts...); err != nil {
			t.Fatalf("create %s: %v", log, err)
		}
		if _, err := alice.SetSchema(ctx, log, "status.set", json.RawMessage(`{"type":"object"}`), contract.EffectMerge); err != nil {
			t.Fatalf("schema %s: %v", log, err)
		}
		if _, err := alice.CreateThing(ctx, log, "case-1", json.RawMessage(`{"n":0}`)); err != nil {
			t.Fatalf("birth %s: %v", log, err)
		}
		if _, err := alice.Append(ctx, log, "case-1", "status.set", []byte(`{"status":"open"}`)); err != nil {
			t.Fatalf("append %s: %v", log, err)
		}
	}
	seed("control")
	seed("audit", client.WithHistory(contract.HistoryPreserved))

	// The control log compacts to one snapshot — the sweep is live.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if ops := replayOf(ctx, t, alice, "control", "case-1"); len(ops) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the sweep never compacted the control log")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The preserved log, swept by the same timer, keeps its trail — give
	// it several more ticks past the control's compaction to be sure the
	// sweep has been through it too.
	time.Sleep(250 * time.Millisecond)
	if ops := replayOf(ctx, t, alice, "audit", "case-1"); len(ops) != 2 {
		t.Fatalf("the sweep touched a preserved log: %d ops", len(ops))
	}
}
