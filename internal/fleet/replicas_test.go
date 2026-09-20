package fleet_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/fleet"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/node"
)

// TestNodeAtTwoReplicas is research 010's question 7 (chronicle-hq
// 01-RESEARCH/010-shared-custody/04-node-at-two-replicas.md), carried from
// chronicle PR #24: two nodes folding one tenant's log at the same time
// must converge on the same state, and a rollup from one must not be
// undone by the other — and the one that loses the race declines at once,
// never by waiting out its replay (the finding 0030 sent to this build).
// The fleet places the first node; the second is started in-process on
// the tenant's service user read from custody, the way a second replica
// runs on another host. Could not pass if the fold's optimistic writes,
// the rollup's expected-sequence guard, or the loser's decline were wrong.
func TestNodeAtTwoReplicas(t *testing.T) {
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

	// The second replica: the tenant's service user — held in custody,
	// read through the first instance's bundle — and a second node.
	bundle, err := mint.ReadBundle(filepath.Join(dir, "bundles", "instance-1"))
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	inc, err := mint.ConnectCreds(f.URL, bundle.ControlCreds, "replica-test")
	if err != nil {
		t.Fatalf("connect instance: %v", err)
	}
	defer inc.Close()
	custody, err := mint.OpenCustody(ctx, inc)
	if err != nil {
		t.Fatalf("custody: %v", err)
	}
	tenant, _, err := custody.Tenant(ctx, "acme")
	if err != nil {
		t.Fatalf("tenant in custody: %v", err)
	}
	nc2, err := mint.ConnectCreds(f.URL, []byte(tenant.ServiceCreds), "chronicle-node-replica-2")
	if err != nil {
		t.Fatalf("connect replica 2: %v", err)
	}
	defer nc2.Close()
	n2, err := node.Start(ctx, nc2, node.Config{})
	if err != nil {
		t.Fatalf("start replica 2: %v", err)
	}
	defer n2.Stop()

	dana, err := client.Connect(f.URL, minted.AdminCreds)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer dana.Close()
	if _, err := dana.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}
	// A merge-effect operation so the fold has real work per op.
	if _, err := dana.DefineType(ctx, "orders", "invoice", client.TypeDefinition{
		Schema:     json.RawMessage(`{"type":"object"}`),
		Operations: map[string]contract.OpDef{"invoice.update": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge}},
	}); err != nil {
		t.Fatalf("define type: %v", err)
	}

	// Both nodes fold every op; the state bucket sees two writers.
	const things, opsPerThing = 5, 40
	var last uint64
	for i := 0; i < things; i++ {
		thing := fmt.Sprintf("invoice.inv-%d", i)
		if _, err := dana.CreateThing(ctx, "orders", thing, json.RawMessage(`{"n":0}`)); err != nil {
			t.Fatalf("create %s: %v", thing, err)
		}
		for k := 1; k <= opsPerThing; k++ {
			ack, err := dana.Append(ctx, "orders", thing, "invoice.update", []byte(fmt.Sprintf(`{"n":%d}`, k)))
			if err != nil {
				t.Fatalf("append %s #%d: %v", thing, k, err)
			}
			last = ack.Seq
		}
	}

	// Convergence: every thing's state reaches its last op with the value
	// the ops say, whichever node wrote it last.
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; i < things; i++ {
		thing := fmt.Sprintf("invoice.inv-%d", i)
		for {
			sv, err := dana.State(ctx, "orders", thing)
			var st struct {
				N int `json:"n"`
			}
			if err == nil {
				_ = json.Unmarshal(sv.State, &st)
			}
			if err == nil && st.N == opsPerThing {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never converged: seq %d, n=%d, %v (last append seq %d)", thing, sv.Seq, st.N, err, last)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Rollups from both sides at once: the guard lets exactly one land per
	// thing, the other reports it lost the race or found nothing to do —
	// never an error, never a second snapshot, and never by waiting out
	// its replay: every answer arrives well inside the old replay budget.
	var wg sync.WaitGroup
	results := make(chan client.ThingRollupResponse, 2*things)
	errs := make(chan error, 2*things)
	started := time.Now()
	for i := 0; i < things; i++ {
		thing := fmt.Sprintf("invoice.inv-%d", i)
		for r := 0; r < 2; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := dana.RollupThing(ctx, "orders", thing)
				if err != nil {
					errs <- fmt.Errorf("%s: %w", thing, err)
					return
				}
				results <- resp
			}()
		}
	}
	wg.Wait()
	elapsed := time.Since(started)
	close(errs)
	close(results)
	// Errors are collected, not fatal here: the integrity checks below
	// must run regardless, so one run says both whether the log stayed
	// sound and whether the losing replica declined cleanly.
	var rollupErrs []error
	for err := range errs {
		rollupErrs = append(rollupErrs, err)
	}
	rolled := 0
	for r := range results {
		if r.Rolled {
			rolled++
		}
	}
	if rolled > things {
		t.Fatalf("%d rollups landed for %d things: a race was not guarded", rolled, things)
	}
	for i := 0; i < things; i++ {
		thing := fmt.Sprintf("invoice.inv-%d", i)
		ops, err := dana.Replay(ctx, "orders", thing)
		if err != nil {
			t.Fatalf("replay %s: %v", thing, err)
		}
		snapshots := 0
		for _, op := range ops {
			if op.Type == contract.OpTypeSnapshot {
				snapshots++
			}
		}
		if snapshots > 1 {
			t.Fatalf("%s carries %d snapshots after concurrent rollups", thing, snapshots)
		}
		// And the compacted history still folds to the same answer.
		sv, err := dana.State(ctx, "orders", thing)
		var st struct {
			N int `json:"n"`
		}
		if err != nil || json.Unmarshal(sv.State, &st) != nil || st.N != opsPerThing {
			t.Fatalf("%s after rollup: state %s, %v", thing, sv.State, err)
		}
	}
	t.Logf("two replicas folded %d ops over %d things; %d of %d concurrent rollups landed in %s", things*opsPerThing, things, rolled, 2*things, elapsed)
	if len(rollupErrs) > 0 {
		t.Fatalf("%d of %d concurrent rollups errored instead of declining (log integrity held: no double snapshot, state correct): %v", len(rollupErrs), 2*things, rollupErrs)
	}
	if elapsed > 8*time.Second {
		t.Fatalf("the concurrent rollups took %s: a loser waited out its replay instead of declining", elapsed)
	}
}
