package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
)

// TestGuardedAppend proves 0018's contract: an opt-in expected-sequence
// guard on plain Append, server-enforced, with the typed refusal and the
// same retry recovery birth and save carry.
func TestGuardedAppend(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := alice.CreateLog(ctx, "ledger", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	birth, err := alice.CreateThing(ctx, "ledger", "acct-1", json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("create thing: %v", err)
	}

	// A guarded append at the observed head lands.
	first, err := alice.Append(ctx, "ledger", "acct-1", "n.add", []byte(`{"v":1}`), client.WithExpectedSeq(birth.Seq))
	if err != nil {
		t.Fatalf("guarded append: %v", err)
	}
	if first.Seq <= birth.Seq {
		t.Fatalf("guarded append landed at %d, birth at %d", first.Seq, birth.Seq)
	}

	// The same guard re-used: the thing moved, and the refusal is typed.
	if _, err := alice.Append(ctx, "ledger", "acct-1", "n.add", []byte(`{"v":2}`), client.WithExpectedSeq(birth.Seq)); !errors.Is(err, client.ErrThingMoved) {
		t.Fatalf("expected ErrThingMoved, got %v", err)
	}

	// A retried guarded append with a pinned op ID reports the original
	// landing — the guard fires before dedup, and the recovery reads
	// through it.
	retry, err := alice.Append(ctx, "ledger", "acct-1", "n.add", []byte(`{"v":1}`), client.WithExpectedSeq(birth.Seq), client.WithOpID(first.OpID))
	if err != nil {
		t.Fatalf("guarded retry: %v", err)
	}
	if retry.Seq != first.Seq {
		t.Fatalf("retry landed elsewhere: %d vs %d", retry.Seq, first.Seq)
	}

	// Two racing guarded appends at the same observed head: the server
	// lets exactly one through.
	head := first.Seq
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = alice.Append(ctx, "ledger", "acct-1", "race.op", []byte(`{}`), client.WithExpectedSeq(head))
		}()
	}
	wg.Wait()
	wins := 0
	for _, e := range errs {
		switch {
		case e == nil:
			wins++
		case errors.Is(e, client.ErrThingMoved):
		default:
			t.Fatalf("racer failed oddly: %v", e)
		}
	}
	if wins != 1 {
		t.Fatalf("%d racers won, want exactly 1", wins)
	}

	// An unguarded append still lands regardless of history — the
	// default is unchanged.
	if _, err := alice.Append(ctx, "ledger", "acct-1", "n.add", []byte(`{"v":9}`)); err != nil {
		t.Fatalf("unguarded append: %v", err)
	}

	// Guard 0 is the birth guard: on an occupied subject the thing has
	// moved by definition.
	if _, err := alice.Append(ctx, "ledger", "acct-1", "n.add", []byte(`{"v":3}`), client.WithExpectedSeq(0)); !errors.Is(err, client.ErrThingMoved) {
		t.Fatalf("expected ErrThingMoved on guard 0, got %v", err)
	}

	// The option belongs to Append alone: a birth guards at 0 by
	// definition, a save guards at upTo — both refuse it loudly.
	if _, err := alice.CreateThing(ctx, "ledger", "acct-2", nil, client.WithExpectedSeq(1)); err == nil {
		t.Fatal("birth accepted WithExpectedSeq")
	}
	if _, err := alice.SaveVersion(ctx, "ledger", "acct-1", json.RawMessage(`{}`), nil, 1, client.WithExpectedSeq(1)); err == nil {
		t.Fatal("save accepted WithExpectedSeq")
	}
}
