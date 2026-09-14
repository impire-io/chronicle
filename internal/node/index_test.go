package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
)

// wantServiceError asserts a refusal by its code — the machine-legible
// half of the headless posture.
func wantServiceError(t *testing.T, err error, code string) {
	t.Helper()
	var serr *client.ServiceError
	if !errors.As(err, &serr) {
		t.Fatalf("want service error %q, got %v", code, err)
	}
	if serr.Code != code {
		t.Fatalf("want error code %q, got %q (%s)", code, serr.Code, serr.Desc)
	}
}

// TestIndexVerbs drives INDEX.DECLARE and INDEX.DELETE: the happy path
// answers the query subject; every refusal is named; declare is
// create-only and delete + declare is the way a declaration changes.
func TestIndexVerbs(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}

	resp, err := alice.DeclareIndex(ctx, "orders", "text", "search", nil)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	if want := client.IndexQuerySubject("orders", "text"); resp.Query != want {
		t.Fatalf("query subject = %q, want %q", resp.Query, want)
	}
	if resp.Query != "CHRON.API.INDEX.QUERY.orders.text" {
		t.Fatalf("query subject grammar drifted: %q", resp.Query)
	}

	// Create-if-absent: the second declare loses, and is told so.
	_, err = alice.DeclareIndex(ctx, "orders", "text", "search", nil)
	wantServiceError(t, err, "index-exists")

	// Write-side strictness: a kind outside the vocabulary is refused.
	_, err = alice.DeclareIndex(ctx, "orders", "holo", "holographic", nil)
	wantServiceError(t, err, "bad-kind")
	// Semantic is in the vocabulary (0016); its config may be absent.
	if _, err = alice.DeclareIndex(ctx, "orders", "vec", "semantic", nil); err != nil {
		t.Fatalf("declare semantic: %v", err)
	}
	// Config belongs to the kind: search refuses one, graph requires one.
	_, err = alice.DeclareIndex(ctx, "orders", "text2", "search", json.RawMessage(`{"anything":true}`))
	wantServiceError(t, err, "bad-config")
	_, err = alice.DeclareIndex(ctx, "orders", "refs", "graph", nil)
	wantServiceError(t, err, "bad-config")

	// Names obey the grammar; logs must exist.
	_, err = alice.DeclareIndex(ctx, "orders", "Bad_Name", "search", nil)
	wantServiceError(t, err, "bad-index-name")
	_, err = alice.DeclareIndex(ctx, "ghost", "text", "search", nil)
	wantServiceError(t, err, "no-such-log")

	// Declaring is configuration: admin only.
	rita, err := client.Wrap(alice.Conn(), "rita")
	if err != nil {
		t.Fatalf("wrap reader: %v", err)
	}
	_, err = rita.DeclareIndex(ctx, "orders", "other", "search", nil)
	wantServiceError(t, err, "forbidden")
	_, err = rita.DeleteIndex(ctx, "orders", "text")
	wantServiceError(t, err, "forbidden")

	// Delete answers honestly on an absent index.
	_, err = alice.DeleteIndex(ctx, "orders", "ghost")
	wantServiceError(t, err, "no-such-index")

	// Delete + declare is how a declaration changes.
	if _, err := alice.DeleteIndex(ctx, "orders", "text"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := alice.DeclareIndex(ctx, "orders", "text", "search", nil); err != nil {
		t.Fatalf("re-declare after delete: %v", err)
	}
}
