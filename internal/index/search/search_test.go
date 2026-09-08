package search_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/index/search"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/internal/node"
)

// setup provisions META the way control does — bucket plus identity
// slice — and starts a node over a plain JetStream server, so the verbs
// that create logs and schemas are the real ones. The indexer under test
// is started by each test itself.
func setup(t *testing.T) (nc *nats.Conn, alice *client.Client) {
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
	admin, _ := json.Marshal(contract.Membership{PublicKey: "UTEST", Role: contract.RoleAdmin})
	if _, err := meta.Put(ctx, contract.MetaMember("alice"), admin); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	reader, _ := json.Marshal(contract.Membership{PublicKey: "UTEST2", Role: contract.RoleReader})
	if _, err := meta.Put(ctx, contract.MetaMember("rita"), reader); err != nil {
		t.Fatalf("seed reader: %v", err)
	}

	n, err := node.Start(ctx, nc, node.Config{})
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

// waitHit polls one query until the wanted thing is the hit, or fails.
func waitHit(ctx context.Context, t *testing.T, c *client.Client, log, index, query, thing string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := c.QueryIndex(ctx, log, index, query, 0, 0)
		if err == nil && len(resp.Hits) == 1 && resp.Hits[0].Thing == thing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("query %q never hit %s: %v %+v", query, thing, err, resp)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSearchEndToEnd is the search kind's contract in one flow: the query
// subject has no responder before the boot replay catches up; after Start
// it finds things by their folded state, keeps up with the live tail, and
// role-checks its callers.
func TestSearchEndToEnd(t *testing.T) {
	nc, alice := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err := alice.SetSchema(ctx, "orders", "status.set", json.RawMessage(`{"type":"object"}`), "merge"); err != nil {
		t.Fatalf("set schema: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"title":"quantum widgets"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice-2", json.RawMessage(`{"title":"boring paperclips"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}

	// The catch-up gate, from the outside: no responder before Start.
	if _, err := alice.QueryIndex(ctx, "orders", "text", "widgets", 0, 0); err == nil ||
		!strings.Contains(err.Error(), "no responder") {
		t.Fatalf("query before the indexer runs: %v", err)
	}

	svc, err := search.Start(ctx, nc, search.Config{Log: "orders", Index: "text"})
	if err != nil {
		t.Fatalf("start indexer: %v", err)
	}
	defer svc.Stop()

	// Boot replay: Start returned, so the pre-existing ops are indexed —
	// no polling needed.
	resp, err := alice.QueryIndex(ctx, "orders", "text", "widgets", 0, 0)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Hits) != 1 || resp.Hits[0].Thing != "invoice-1" || resp.Hits[0].Score <= 0 {
		t.Fatalf("boot replay hits: %+v", resp)
	}

	// The live tail: a merge op changes the state the index sees.
	if _, err := alice.Append(ctx, "orders", "invoice-1", "status.set", []byte(`{"title":"chrono gadgets"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	waitHit(ctx, t, alice, "orders", "text", "gadgets", "invoice-1")

	// An empty query matches everything — the total counts things, not ops.
	resp, err = alice.QueryIndex(ctx, "orders", "text", "", 0, 0)
	if err != nil {
		t.Fatalf("match-all query: %v", err)
	}
	if resp.Total != 2 {
		t.Fatalf("match-all total = %d, want 2", resp.Total)
	}

	// Any registry role may query; a non-member may not.
	rita, err := client.Wrap(alice.Conn(), "rita")
	if err != nil {
		t.Fatalf("wrap reader: %v", err)
	}
	if _, err := rita.QueryIndex(ctx, "orders", "text", "gadgets", 0, 0); err != nil {
		t.Fatalf("reader query: %v", err)
	}
	mallory, err := client.Wrap(alice.Conn(), "mallory")
	if err != nil {
		t.Fatalf("wrap outsider: %v", err)
	}
	if _, err := mallory.QueryIndex(ctx, "orders", "text", "gadgets", 0, 0); err == nil {
		t.Fatal("non-member query answered")
	}
}

// TestSearchFollowsEffects pins the two effect rules: an effect-none op's
// content is invisible to search, and a changed effect makes the index
// suspect — the indexer re-folds and the content becomes findable.
func TestSearchFollowsEffects(t *testing.T) {
	nc, alice := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "notes", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err := alice.SetSchema(ctx, "notes", "note.add", json.RawMessage(`{"type":"object"}`), ""); err != nil {
		t.Fatalf("set schema: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "notes", "n1", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := alice.Append(ctx, "notes", "n1", "note.add", []byte(`{"body":"xyzzy plugh"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}

	svc, err := search.Start(ctx, nc, search.Config{Log: "notes", Index: "text"})
	if err != nil {
		t.Fatalf("start indexer: %v", err)
	}
	defer svc.Stop()

	// Effect none: the op lives in history; search cannot see it.
	resp, err := alice.QueryIndex(ctx, "notes", "text", "xyzzy", 0, 0)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Hits) != 0 {
		t.Fatalf("effect-none content surfaced: %+v", resp)
	}

	// Latest declaration wins: none → merge re-folds the index in place.
	if _, err := alice.SetSchema(ctx, "notes", "note.add", json.RawMessage(`{"type":"object"}`), "merge"); err != nil {
		t.Fatalf("change effect: %v", err)
	}
	waitHit(ctx, t, alice, "notes", "text", "xyzzy", "n1")
}
