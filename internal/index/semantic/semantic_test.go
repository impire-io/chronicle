package semantic_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/index/semantic"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/internal/node"
)

// testProvider is a real OpenAI-compatible /embeddings endpoint serving
// deterministic vectors — keyword counts — so ranking is provable and
// the wire test needs no external dependency. Failures are switchable to
// prove honest degradation.
type testProvider struct {
	*httptest.Server
	calls atomic.Int64
	// failSubstr rejects any request whose input contains it — the
	// partial failure of a real provider (an input refused, a model
	// window overflowed) while other embeds and queries keep working.
	failSubstr atomic.Value // string
}

func startTestProvider(t *testing.T) *testProvider {
	t.Helper()
	p := &testProvider{}
	mux := http.NewServeMux()
	p.failSubstr.Store("")
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		p.calls.Add(1)
		var req struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if substr := p.failSubstr.Load().(string); substr != "" {
			for _, in := range req.Input {
				if strings.Contains(strings.ToLower(in), substr) {
					http.Error(w, "input refused", http.StatusServiceUnavailable)
					return
				}
			}
		}
		type datum struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		}
		var data []datum
		for i, text := range req.Input {
			lower := strings.ToLower(text)
			data = append(data, datum{Index: i, Embedding: []float64{
				float64(strings.Count(lower, "widget")),
				float64(strings.Count(lower, "gadget")),
				0.1, // never a zero vector
			}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	})
	p.Server = httptest.NewServer(mux)
	t.Cleanup(p.Close)
	return p
}

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
	principal, _ := json.Marshal(contract.Principal{ID: "alice"})
	if _, err := meta.Put(ctx, contract.MetaPrincipal("alice"), principal); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
	admin, _ := json.Marshal(contract.Membership{PublicKey: "UTEST", Role: contract.RoleAdmin})
	if _, err := meta.Put(ctx, contract.MetaMember("alice"), admin); err != nil {
		t.Fatalf("seed membership: %v", err)
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

// TestSemanticEndToEnd: fold → embed → query by meaning, ranking by
// best chunk; the live tail re-embeds a changed thing; a failing
// provider marks the new thing unembedded — said in the reply — while
// the embedded corpus keeps serving; recovery drains it.
func TestSemanticEndToEnd(t *testing.T) {
	nc, alice := setup(t)
	provider := startTestProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err := alice.SetSchema(ctx, "orders", "order.update", json.RawMessage(`{"type":"object"}`), "merge"); err != nil {
		t.Fatalf("set schema: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"title":"quantum widget order"}`)); err != nil {
		t.Fatalf("create invoice-1: %v", err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "invoice-2", json.RawMessage(`{"title":"gadget shipment"}`)); err != nil {
		t.Fatalf("create invoice-2: %v", err)
	}
	if _, err := alice.DeclareIndex(ctx, "orders", "meaning", "semantic", nil); err != nil {
		t.Fatalf("declare semantic index: %v", err)
	}

	svc, err := semantic.Start(ctx, nc, semantic.Config{
		Log: "orders", Index: "meaning",
		Provider: semantic.ProviderConfig{BaseURL: provider.URL + "/v1", Model: "test-embed"},
	})
	if err != nil {
		t.Fatalf("start semantic service: %v", err)
	}
	t.Cleanup(svc.Stop)

	resp, err := alice.QuerySemantic(ctx, "orders", "meaning", "widget", 0, 0)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Hits) == 0 || resp.Hits[0].Thing != "invoice-1" || resp.Unembedded != 0 {
		t.Fatalf("widget query = %+v", resp)
	}
	resp, err = alice.QuerySemantic(ctx, "orders", "meaning", "gadget", 0, 0)
	if err != nil || resp.Hits[0].Thing != "invoice-2" {
		t.Fatalf("gadget query = %+v, %v", resp, err)
	}

	// The provider refuses one thing's text — the partial failure of the
	// hits item-21 shape — so the new thing folds but cannot embed: the
	// reply says so while the embedded corpus keeps answering and query
	// embedding still works.
	provider.failSubstr.Store("unembeddable")
	if _, err := alice.CreateThing(ctx, "orders", "invoice-3", json.RawMessage(`{"title":"widget widget widget unembeddable"}`)); err != nil {
		t.Fatalf("create invoice-3: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err = alice.QuerySemantic(ctx, "orders", "meaning", "widget", 0, 0)
		if err == nil && resp.Unembedded == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("degradation never surfaced: %+v %v", resp, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, h := range resp.Hits {
		if h.Thing == "invoice-3" {
			t.Fatalf("unembedded thing served a score: %+v", resp)
		}
	}

	// Recovery drains the backlog: invoice-3 joins the corpus with a
	// near-perfect widget score. (Cosine measures direction, not
	// magnitude — repeating a word does not outrank containing it.)
	provider.failSubstr.Store("")
	deadline = time.Now().Add(15 * time.Second)
	for {
		resp, err = alice.QuerySemantic(ctx, "orders", "meaning", "widget", 0, 0)
		if err == nil && resp.Unembedded == 0 && scoreOf(resp, "invoice-3") > 0.9 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery never completed: %+v %v", resp, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func scoreOf(resp client.SemanticQueryResponse, thing string) float64 {
	for _, h := range resp.Hits {
		if h.Thing == thing {
			return h.Score
		}
	}
	return -1
}
