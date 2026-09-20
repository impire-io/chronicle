package up_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/devdir"
	"github.com/impire-io/chronicle/up"
)

// TestUpServesOneTenant is the open form's could-not-succeed-if-broken
// read: a folded state and a served index over the quick start, with
// nothing minted — and the identity and the declared index both survive
// a restart of the same dir.
func TestUpServesOneTenant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	dir := t.TempDir()

	l, err := up.Up(ctx, up.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer l.Stop()
	if url, err := devdir.ReadClientURL(dir); err != nil || url != l.URL {
		t.Fatalf("recorded url %q (%v), want %q", url, err, l.URL)
	}

	// Nobody else may connect: the one account has one user.
	if _, err := client.ConnectWith(l.URL, "stranger"); err == nil {
		t.Fatal("an anonymous connection was accepted")
	}

	admin, err := client.ConnectNkeyFile(l.URL, devdir.UserNkeyPath(dir), devdir.LocalPrincipal)
	if err != nil {
		t.Fatalf("connect as admin: %v", err)
	}
	defer admin.Close()
	if _, err := admin.CreateLog(ctx, "orders", "the orders"); err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err := admin.DeclareIndex(ctx, "orders", "text", contract.IndexKindSearch, nil); err != nil {
		t.Fatalf("declare index: %v", err)
	}
	if _, err := admin.CreateThing(ctx, "orders", "note-1", json.RawMessage(`{"title":"quantum widgets"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	waitState(ctx, t, admin, "orders", "note-1", "quantum")
	waitHit(ctx, t, admin, "orders", "text", "widgets", "note-1")

	// The principal is checked at the node: a non-member's assertion is
	// refused even on the same key.
	stranger, err := client.ConnectNkeyFile(l.URL, devdir.UserNkeyPath(dir), "mallory")
	if err != nil {
		t.Fatalf("connect as mallory: %v", err)
	}
	defer stranger.Close()
	if _, err := stranger.CreateLog(ctx, "theirs", ""); err == nil || !strings.Contains(err.Error(), "not a member") {
		t.Fatalf("non-member create log: %v", err)
	}

	// Deleting the index retires its process: the query has no responder.
	if _, err := admin.DeleteIndex(ctx, "orders", "text"); err != nil {
		t.Fatalf("delete index: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := admin.QueryIndex(ctx, "orders", "text", "widgets", 10, 0)
		if err != nil && strings.Contains(err.Error(), "no responder") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retired index still answers: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := admin.DeclareIndex(ctx, "orders", "text", contract.IndexKindSearch, nil); err != nil {
		t.Fatalf("re-declare index: %v", err)
	}
	waitHit(ctx, t, admin, "orders", "text", "widgets", "note-1")

	// A restart of the same dir: same user, same url file, the declared
	// index placed again by the node's boot re-derivation.
	admin.Close()
	l.Stop()
	l2, err := up.Up(ctx, up.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up again: %v", err)
	}
	defer l2.Stop()
	admin2, err := client.ConnectNkeyFile(l2.URL, devdir.UserNkeyPath(dir), devdir.LocalPrincipal)
	if err != nil {
		t.Fatalf("reconnect as admin: %v", err)
	}
	defer admin2.Close()
	waitHit(ctx, t, admin2, "orders", "text", "widgets", "note-1")
}

func waitState(ctx context.Context, t *testing.T, c *client.Client, log, thing, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := c.State(ctx, log, thing)
		if err == nil && strings.Contains(string(st.State), want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state of %s never held %q: %v", thing, want, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitHit(ctx context.Context, t *testing.T, c *client.Client, log, index, query, wantThing string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := c.QueryIndex(ctx, log, index, query, 10, 0)
		if err == nil {
			for _, h := range resp.Hits {
				if h.Thing == wantThing {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("index %s never returned %s for %q: %v", index, wantThing, query, err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("query: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
