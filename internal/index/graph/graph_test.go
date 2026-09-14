package graph

import (
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

func testRun(t *testing.T, rules ...contract.GraphEdgeRule) *graphRun {
	t.Helper()
	return newGraphRun("orders", rules, slog.Default())
}

// TestExtractionWalksPathsAndArrays: dotted paths, arrays traversed
// element-wise, strings at the leaf, everything else quietly nothing.
func TestExtractionWalksPathsAndArrays(t *testing.T) {
	r := testRun(t,
		contract.GraphEdgeRule{Field: "customer"},
		contract.GraphEdgeRule{Field: "lines.product", Label: "product"},
	)
	state := json.RawMessage(`{
		"customer": "cust-1",
		"lines": [
			{"product": "prod-a", "qty": 2},
			{"product": "prod-b"},
			{"note": "no product field"},
			{"product": 42}
		],
		"total": 99
	}`)
	edges := r.extract("invoice-1", state)
	want := map[contract.GraphEdge]bool{
		{From: "invoice-1", To: "cust-1", Label: "customer"}: true,
		{From: "invoice-1", To: "prod-a", Label: "product"}:  true,
		{From: "invoice-1", To: "prod-b", Label: "product"}:  true,
	}
	if len(edges) != len(want) {
		t.Fatalf("extracted %v, want %d edges", edges, len(want))
	}
	for _, e := range edges {
		if !want[e] {
			t.Fatalf("unexpected edge %+v", e)
		}
	}
}

// TestUpsertRewiresWholesale: out-edges are a pure function of current
// state — a changed reference replaces the old edge everywhere, reverse
// index included.
func TestUpsertRewiresWholesale(t *testing.T) {
	r := testRun(t, contract.GraphEdgeRule{Field: "customer"})
	r.Upsert("invoice-1", json.RawMessage(`{"customer":"cust-1"}`))
	r.Upsert("invoice-2", json.RawMessage(`{"customer":"cust-1"}`))

	in := r.neighbors(client.GraphQueryRequest{Thing: "cust-1", Direction: contract.GraphDirectionIn})
	if in.Total != 2 {
		t.Fatalf("cust-1 in-edges = %d, want 2", in.Total)
	}

	// invoice-1 moves to cust-2: the old edge must vanish from both maps.
	r.Upsert("invoice-1", json.RawMessage(`{"customer":"cust-2"}`))
	in = r.neighbors(client.GraphQueryRequest{Thing: "cust-1", Direction: contract.GraphDirectionIn})
	if in.Total != 1 || in.Edges[0].From != "invoice-2" {
		t.Fatalf("after rewire cust-1 in-edges = %+v", in)
	}
	out := r.neighbors(client.GraphQueryRequest{Thing: "invoice-1", Direction: contract.GraphDirectionOut})
	if out.Total != 1 || out.Edges[0].To != "cust-2" {
		t.Fatalf("after rewire invoice-1 out-edges = %+v", out)
	}

	// A state without the field clears the thing's edges entirely.
	r.Upsert("invoice-1", json.RawMessage(`{"paid":true}`))
	out = r.neighbors(client.GraphQueryRequest{Thing: "invoice-1"})
	if out.Total != 0 {
		t.Fatalf("cleared thing still has edges: %+v", out)
	}
}

// TestWalkIsBreadthFirstCycleSafeAndCapped.
func TestWalkIsBreadthFirstCycleSafeAndCapped(t *testing.T) {
	r := testRun(t, contract.GraphEdgeRule{Field: "next"})
	// a → b → c → a: a cycle, plus a side branch b → d via another label
	// that a label filter must exclude.
	r.Upsert("a", json.RawMessage(`{"next":"b"}`))
	r.Upsert("b", json.RawMessage(`{"next":"c"}`))
	r.Upsert("c", json.RawMessage(`{"next":"a"}`))

	walk := r.walk(client.GraphQueryRequest{Thing: "a", Depth: 10})
	if !walk.DepthCapped {
		t.Fatal("depth 10 should report the cap")
	}
	if len(walk.Things) != 2 {
		t.Fatalf("cycle walk visited %+v, want b and c once each", walk.Things)
	}
	if walk.Things[0].Thing != "b" || walk.Things[0].Depth != 1 || walk.Things[1].Thing != "c" || walk.Things[1].Depth != 2 {
		t.Fatalf("walk order wrong: %+v", walk.Things)
	}

	one := r.walk(client.GraphQueryRequest{Thing: "a", Depth: 1})
	if len(one.Things) != 1 || one.Things[0].Thing != "b" {
		t.Fatalf("depth-1 walk = %+v", one.Things)
	}
}

// TestParseGraphConfigIsWriteSideStrict lives beside the evaluator it
// guards: rules must exist and be well-formed, unknown keys refused.
func TestParseGraphConfigIsWriteSideStrict(t *testing.T) {
	good := json.RawMessage(`{"edges":[{"field":"customer"}]}`)
	if _, err := contract.ParseGraphConfig(good); err != nil {
		t.Fatalf("good config refused: %v", err)
	}
	for name, raw := range map[string]json.RawMessage{
		"empty":        nil,
		"no rules":     json.RawMessage(`{"edges":[]}`),
		"empty path":   json.RawMessage(`{"edges":[{"field":""}]}`),
		"dotted hole":  json.RawMessage(`{"edges":[{"field":"a..b"}]}`),
		"unknown keys": json.RawMessage(`{"edges":[{"field":"a"}],"mystery":true}`),
		"duplicate":    json.RawMessage(`{"edges":[{"field":"a"},{"field":"a"}]}`),
	} {
		if _, err := contract.ParseGraphConfig(raw); err == nil {
			t.Fatalf("%s config accepted", name)
		}
	}
}
