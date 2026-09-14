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

// TestDeclaredIndexServes is 0012's supervision floor: declaring an index
// through the API is enough — the fleet watches META, places the indexer,
// and the query subject answers once caught up; deleting the declaration
// retires it and the subject goes silent again. No scheduler exists yet;
// this is its stand-in seam.
func TestDeclaredIndexServes(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer f.Stop()

	ctrlCreds, err := os.ReadFile(devdir.ControlCredsPath(dir))
	if err != nil {
		t.Fatalf("control creds: %v", err)
	}
	ctrl, err := client.ConnectControlCreds(f.URL, ctrlCreds)
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	minted, err := ctrl.MintTenant(ctx, "acme", "dana")
	ctrl.Close()
	if err != nil {
		t.Fatalf("mint tenant: %v", err)
	}
	dana, err := client.Connect(f.URL, minted.AdminCreds)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer dana.Close()

	if _, err := dana.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	if _, err := dana.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"title":"quantum widgets"}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	resp, err := dana.DeclareIndex(ctx, "orders", "text", "search", nil)
	if err != nil {
		t.Fatalf("declare index: %v", err)
	}
	if resp.Query == "" {
		t.Fatalf("declare answered no query subject: %+v", resp)
	}

	// The supervisor places the workload; the endpoint appears once the
	// index is caught up.
	deadline := time.Now().Add(15 * time.Second)
	for {
		qr, err := dana.QueryIndex(ctx, "orders", "text", "widgets", 0, 0)
		if err == nil && len(qr.Hits) == 1 && qr.Hits[0].Thing == "invoice-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("declared index never served: %v %+v", err, qr)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Retiring the declaration takes the responder off the wire.
	if _, err := dana.DeleteIndex(ctx, "orders", "text"); err != nil {
		t.Fatalf("delete index: %v", err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		if _, err := dana.QueryIndex(ctx, "orders", "text", "widgets", 0, 0); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retired index still answers")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDeclaredGraphServes is the graph kind's floor (0015): declared
// edge rules materialize as adjacency the moment the workload catches
// up, a state change rewires edges wholesale, and deleting the
// declaration takes the responder away.
func TestDeclaredGraphServes(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer f.Stop()

	ctrlCreds, err := os.ReadFile(devdir.ControlCredsPath(dir))
	if err != nil {
		t.Fatalf("control creds: %v", err)
	}
	ctrl, err := client.ConnectControlCreds(f.URL, ctrlCreds)
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	minted, err := ctrl.MintTenant(ctx, "acme", "dana")
	ctrl.Close()
	if err != nil {
		t.Fatalf("mint tenant: %v", err)
	}
	dana, err := client.Connect(f.URL, minted.AdminCreds)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer dana.Close()

	if _, err := dana.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	// A merge-effect type so a later append can move the reference.
	if _, err := dana.SetSchema(ctx, "orders", "order.update", json.RawMessage(`{"type":"object"}`), "merge"); err != nil {
		t.Fatalf("set schema: %v", err)
	}
	if _, err := dana.CreateThing(ctx, "orders", "invoice-1", json.RawMessage(`{"customer":"cust-1"}`)); err != nil {
		t.Fatalf("create invoice-1: %v", err)
	}
	if _, err := dana.CreateThing(ctx, "orders", "invoice-2", json.RawMessage(`{"customer":"cust-1"}`)); err != nil {
		t.Fatalf("create invoice-2: %v", err)
	}

	if _, err := dana.DeclareIndex(ctx, "orders", "refs", "graph", json.RawMessage(`{"edges":[{"field":"customer"}]}`)); err != nil {
		t.Fatalf("declare graph index: %v", err)
	}
	// A config-less graph declaration is refused write-side strict.
	if _, err := dana.DeclareIndex(ctx, "orders", "naked", "graph", nil); err == nil {
		t.Fatal("graph declaration without config accepted")
	}

	// The workload catches up and both directions answer.
	deadline := time.Now().Add(15 * time.Second)
	for {
		in, err := dana.GraphNeighbors(ctx, "orders", "refs", client.GraphQueryRequest{Thing: "cust-1", Direction: "in"})
		if err == nil && in.Total == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("declared graph never served: %v %+v", err, in)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// A merge op moves the reference; the live tail rewires the edges.
	if _, err := dana.Append(ctx, "orders", "invoice-1", "order.update", []byte(`{"customer":"cust-2"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		in, err := dana.GraphNeighbors(ctx, "orders", "refs", client.GraphQueryRequest{Thing: "cust-2", Direction: "in"})
		if err == nil && in.Total == 1 && in.Edges[0].From == "invoice-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("live tail never rewired the edge: %v %+v", err, in)
		}
		time.Sleep(50 * time.Millisecond)
	}
	walk, err := dana.GraphWalk(ctx, "orders", "refs", client.GraphQueryRequest{Thing: "invoice-1", Depth: 1})
	if err != nil || len(walk.Things) != 1 || walk.Things[0].Thing != "cust-2" {
		t.Fatalf("walk = %+v, %v", walk, err)
	}

	// Retiring the declaration takes the responder off the wire.
	if _, err := dana.DeleteIndex(ctx, "orders", "refs"); err != nil {
		t.Fatalf("delete index: %v", err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		if _, err := dana.GraphNeighbors(ctx, "orders", "refs", client.GraphQueryRequest{Thing: "cust-2", Direction: "in"}); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retired graph index still answers")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
