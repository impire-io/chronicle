package fleet_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/fleet"
)

// TestWalkingSkeleton drives the whole floor in one flow, the same one the
// CLI drives: up → mint → create log → publish with pre-flight → fold →
// state bucket → read and replay — then a restart, because a fleet that
// only works on its first boot is a demo, not a fleet.
func TestWalkingSkeleton(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			f.Stop()
		}
	}()

	// FR-01: mint a tenant through control, over the control account.
	ctrlCreds, err := os.ReadFile(devdir.ControlCredsPath(dir))
	if err != nil {
		t.Fatalf("control creds: %v", err)
	}
	ctrl, err := client.ConnectControlCreds(f.URL, ctrlCreds)
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	minted, err := ctrl.MintTenant(ctx, "acme", "dana")
	if err != nil {
		ctrl.Close()
		t.Fatalf("mint tenant: %v", err)
	}
	if minted.Admin != "dana" || len(minted.AdminCreds) == 0 {
		ctrl.Close()
		t.Fatalf("mint response incomplete: %+v", minted)
	}
	// The same name again is refused.
	if _, err := ctrl.MintTenant(ctx, "acme", ""); err == nil {
		ctrl.Close()
		t.Fatal("duplicate tenant minted")
	}
	ctrl.Close()

	// The admin connects with the handed-back creds — the member baseline.
	dana, err := client.Connect(f.URL, minted.AdminCreds)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	if dana.Author() != "dana" {
		t.Fatalf("author from creds = %q", dana.Author())
	}

	// FR-02 → FR-05: the spine.
	if _, err := dana.CreateLog(ctx, "orders", "orders log"); err != nil {
		t.Fatalf("create log: %v", err)
	}
	birth, err := dana.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"total":1}`))
	if err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := dana.Append(ctx, "orders", "invoice-1", "comment.add", []byte(`{"body":"hello"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	waitState := func(c *client.Client, wantSeq uint64) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			sv, err := c.State(ctx, "orders", "invoice-1")
			if err == nil && sv.Seq >= wantSeq {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("state never reached seq %d: %v", wantSeq, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitState(dana, birth.Seq)
	ops, err := dana.Replay(ctx, "orders", "invoice-1")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("replay returned %d ops", len(ops))
	}
	dana.Close()

	// FR-06's other half: the fleet comes back. Stop everything, boot from
	// the same dir, and the tenant's node must be running again — state
	// readable, verbs answered.
	f.Stop()
	stopped = true

	f2, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up again: %v", err)
	}
	defer f2.Stop()

	dana2, err := client.Connect(f2.URL, minted.AdminCreds)
	if err != nil {
		t.Fatalf("reconnect admin: %v", err)
	}
	defer dana2.Close()
	waitState(dana2, birth.Seq)
	if _, err := dana2.CreateLog(ctx, "second", ""); err != nil {
		t.Fatalf("create log after restart: %v", err)
	}
	// And the log's history survived the restart, warts and all.
	ops, err = dana2.Replay(ctx, "orders", "invoice-1")
	if err != nil {
		t.Fatalf("replay after restart: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("history lost across restart: %d ops", len(ops))
	}
	if !errors.Is(ctx.Err(), nil) {
		t.Fatal("test overran its budget")
	}
}
