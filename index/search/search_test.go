package search_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/index/search"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/node"
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
	if _, err := alice.DefineType(ctx, "orders", "invoice", client.TypeDefinition{
		Schema:     json.RawMessage(`{"type":"object"}`),
		Operations: map[string]contract.OpDef{"status.set": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge}},
	}); err != nil {
		t.Fatalf("define type: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"title":"quantum widgets"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice.invoice-2", json.RawMessage(`{"title":"boring paperclips"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}

	// The catch-up gate, from the outside: no responder before Start.
	if _, err := alice.QueryIndex(ctx, "orders", "text", "widgets", 0, 0); err == nil ||
		!strings.Contains(err.Error(), "no responder") {
		t.Fatalf("query before the indexer runs: %v", err)
	}

	if _, err := alice.DeclareIndex(ctx, "orders", "text", "search", nil); err != nil {
		t.Fatalf("declare index: %v", err)
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
	if len(resp.Hits) != 1 || resp.Hits[0].Thing != "invoice.invoice-1" || resp.Hits[0].Score <= 0 {
		t.Fatalf("boot replay hits: %+v", resp)
	}

	// The live tail: a merge op changes the state the index sees.
	if _, err := alice.Append(ctx, "orders", "invoice.invoice-1", "status.set", []byte(`{"title":"chrono gadgets"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	waitHit(ctx, t, alice, "orders", "text", "gadgets", "invoice.invoice-1")

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
	if _, err := alice.DefineType(ctx, "notes", "note", client.TypeDefinition{
		Schema:     json.RawMessage(`{"type":"object"}`),
		Operations: map[string]contract.OpDef{"note.add": {Schema: json.RawMessage(`{"type":"object"}`)}},
	}); err != nil {
		t.Fatalf("define type: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "notes", "note.n1", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := alice.Append(ctx, "notes", "note.n1", "note.add", []byte(`{"body":"xyzzy plugh"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}

	if _, err := alice.DeclareIndex(ctx, "notes", "text", "search", nil); err != nil {
		t.Fatalf("declare index: %v", err)
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
	if _, err := alice.DefineType(ctx, "notes", "note", client.TypeDefinition{
		Schema:     json.RawMessage(`{"type":"object"}`),
		Operations: map[string]contract.OpDef{"note.add": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge}},
	}); err != nil {
		t.Fatalf("change effect: %v", err)
	}
	waitHit(ctx, t, alice, "notes", "text", "xyzzy", "note.n1")
}

// TestSearchOpsSource proves 0020's contract for the search kind: an
// ops-sourced index finds text that lives only in history — effect-none
// ops — with thing-level hits scored by the best op, an honest thing
// total, and the types narrowing honored.
func TestSearchOpsSource(t *testing.T) {
	nc, alice := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "items", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	// note.add stays effect-none: its content is invisible to any
	// state-sourced index, which is exactly the gap the ops source fills.
	if _, err := alice.DefineType(ctx, "items", "item", client.TypeDefinition{
		Schema:     json.RawMessage(`{"type":"object"}`),
		Operations: map[string]contract.OpDef{"note.add": {Schema: json.RawMessage(`{"type":"object"}`)}},
	}); err != nil {
		t.Fatalf("define type: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "items", "item.item-1", json.RawMessage(`{"title":"a widget"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "items", "item.item-2", json.RawMessage(`{"title":"a gadget"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := alice.Append(ctx, "items", "item.item-1", "note.add", []byte(`{"body":"the flux capacitor hums"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := alice.Append(ctx, "items", "item.item-1", "note.add", []byte(`{"body":"the flux capacitor still hums"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := alice.Append(ctx, "items", "item.item-2", "note.add", []byte(`{"body":"nothing flux about this one"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}

	if _, err := alice.DeclareIndex(ctx, "items", "trail", "search", json.RawMessage(`{"source":"ops"}`)); err != nil {
		t.Fatalf("declare ops index: %v", err)
	}
	svc, err := search.Start(ctx, nc, search.Config{Log: "items", Index: "trail"})
	if err != nil {
		t.Fatalf("start indexer: %v", err)
	}
	defer svc.Stop()

	// Text living only in history is findable; hits name things, one per
	// thing however many ops matched, best op first.
	resp, err := alice.QueryIndex(ctx, "items", "trail", "flux", 0, 0)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if resp.Total != 2 || len(resp.Hits) != 2 {
		t.Fatalf("flux hits: %+v", resp)
	}
	seen := map[string]bool{}
	for _, h := range resp.Hits {
		seen[h.Thing] = true
	}
	if !seen["item.item-1"] || !seen["item.item-2"] {
		t.Fatalf("hits must name the things: %+v", resp.Hits)
	}

	// The live tail: a fresh op becomes findable without any effect help.
	if _, err := alice.Append(ctx, "items", "item.item-2", "note.add", []byte(`{"body":"zorble"}`)); err != nil {
		t.Fatalf("append live: %v", err)
	}
	waitHit(ctx, t, alice, "items", "trail", "zorble", "item.item-2")

	// The types narrowing: an index reading only note.add cannot see the
	// birth snapshots' state text.
	if _, err := alice.DeclareIndex(ctx, "items", "notes-only", "search", json.RawMessage(`{"source":"ops","types":["note.add"]}`)); err != nil {
		t.Fatalf("declare narrowed index: %v", err)
	}
	svc2, err := search.Start(ctx, nc, search.Config{Log: "items", Index: "notes-only"})
	if err != nil {
		t.Fatalf("start narrowed indexer: %v", err)
	}
	defer svc2.Stop()
	resp, err = alice.QueryIndex(ctx, "items", "notes-only", "widget", 0, 0)
	if err != nil {
		t.Fatalf("narrowed query: %v", err)
	}
	if len(resp.Hits) != 0 {
		t.Fatalf("a types-narrowed index saw another type's op: %+v", resp)
	}
	if resp, err = alice.QueryIndex(ctx, "items", "notes-only", "flux", 0, 0); err != nil || resp.Total != 2 {
		t.Fatalf("narrowed flux hits: %v %+v", err, resp)
	}
}

// TestParseSearchConfigIsWriteSideStrict pins the search config grammar
// (0020): the source vocabulary, the types narrowing, and nothing else —
// search stays no-knobs.
func TestParseSearchConfigIsWriteSideStrict(t *testing.T) {
	if _, err := contract.ParseSearchConfig(nil); err != nil {
		t.Fatalf("absent config: %v", err)
	}
	if cfg, err := contract.ParseSearchConfig(json.RawMessage(`{"source":"ops"}`)); err != nil || cfg.Source != "ops" {
		t.Fatalf("ops source: %v %+v", err, cfg)
	}
	if _, err := contract.ParseSearchConfig(json.RawMessage(`{"source":"tape"}`)); err == nil {
		t.Fatal("unknown source accepted")
	}
	if _, err := contract.ParseSearchConfig(json.RawMessage(`{"analyzer":"keyword"}`)); err == nil {
		t.Fatal("an analysis knob accepted — search is no-knobs")
	}
	if _, err := contract.ParseSearchConfig(json.RawMessage(`{"types":["note.add"]}`)); err == nil {
		t.Fatal("types accepted on the state source")
	}
	if _, err := contract.ParseSearchConfig(json.RawMessage(`{"source":"ops","types":["note.add"]}`)); err != nil {
		t.Fatalf("ops types narrowing refused: %v", err)
	}
	if _, err := contract.ParseSearchConfig(json.RawMessage(`{"source":"ops","types":[""]}`)); err == nil {
		t.Fatal("empty type accepted")
	}
}

// lineCatcher collects log lines so a test can assert which boot path a
// service took.
type lineCatcher struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *lineCatcher) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lineCatcher) has(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(l.buf.String(), sub)
}

// TestSearchBootsFromStateCheckpoint proves 0023 § 4: a state-sourced
// indexer boots from the state index's {seq, state} checkpoint when the
// fold watermark names the current declarations — and answers exactly as
// a full replay would — while a stale watermark voids the shortcut and
// the pass replays from sequence 1, the suspicion rule.
func TestSearchBootsFromStateCheckpoint(t *testing.T) {
	nc, alice := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err := alice.DefineType(ctx, "orders", "invoice", client.TypeDefinition{
		Schema:     json.RawMessage(`{"type":"object"}`),
		Operations: map[string]contract.OpDef{"status.set": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge}},
	}); err != nil {
		t.Fatalf("define type: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"title":"quantum widgets"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice.invoice-2", json.RawMessage(`{"title":"plain paperclips"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	moved, err := alice.Append(ctx, "orders", "invoice.invoice-1", "status.set", []byte(`{"title":"chrono gadgets"}`))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	// The node's state index must hold the checkpoint before the indexer
	// boots.
	deadline := time.Now().Add(10 * time.Second)
	for {
		sv, err := alice.State(ctx, "orders", "invoice.invoice-1")
		if err == nil && sv.Seq == moved.Seq {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state never caught up: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}

	if _, err := alice.DeclareIndex(ctx, "orders", "text", "search", nil); err != nil {
		t.Fatalf("declare index: %v", err)
	}
	catcher := &lineCatcher{}
	svc, err := search.Start(ctx, nc, search.Config{Log: "orders", Index: "text", Logger: slog.New(slog.NewTextHandler(catcher, nil))})
	if err != nil {
		t.Fatalf("start indexer: %v", err)
	}
	waitHit(ctx, t, alice, "orders", "text", "gadgets", "invoice.invoice-1")
	if !catcher.has("state checkpoint seeded") {
		t.Fatal("the boot did not use the state checkpoint")
	}
	svc.Stop()

	// A stale watermark voids the shortcut: the pass replays whole and
	// still answers the same.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	states, err := js.KeyValue(ctx, contract.StateBucket("orders"))
	if err != nil {
		t.Fatalf("open state bucket: %v", err)
	}
	stale, _ := json.Marshal(contract.FoldWatermark{Declarations: "stale"})
	if _, err := states.Put(ctx, contract.StateFoldKey, stale); err != nil {
		t.Fatalf("tamper watermark: %v", err)
	}
	catcher2 := &lineCatcher{}
	svc2, err := search.Start(ctx, nc, search.Config{Log: "orders", Index: "text", Logger: slog.New(slog.NewTextHandler(catcher2, nil))})
	if err != nil {
		t.Fatalf("restart indexer: %v", err)
	}
	defer svc2.Stop()
	waitHit(ctx, t, alice, "orders", "text", "gadgets", "invoice.invoice-1")
	if catcher2.has("state checkpoint seeded") {
		t.Fatal("a stale watermark must void the checkpoint")
	}
}
