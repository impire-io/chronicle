package fleet_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/devdir"
	"github.com/impire-io/chronicle/internal/fleet"
	"github.com/impire-io/chronicle/internal/index/semantic"
	"github.com/impire-io/chronicle/internal/mint"
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
	birth, err := dana.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"total":1}`))
	if err != nil {
		t.Fatalf("create thing: %v", err)
	}
	if _, err := dana.Append(ctx, "orders", "invoice.invoice-1", "comment.add", []byte(`{"body":"hello"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	waitState := func(c *client.Client, wantSeq uint64) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			sv, err := c.State(ctx, "orders", "invoice.invoice-1")
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
	ops, err := dana.Replay(ctx, "orders", "invoice.invoice-1")
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
	ops, err = dana2.Replay(ctx, "orders", "invoice.invoice-1")
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
	if _, err := dana.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"title":"quantum widgets"}`)); err != nil {
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
		if err == nil && len(qr.Hits) == 1 && qr.Hits[0].Thing == "invoice.invoice-1" {
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
	// A merge-effect operation so a later append can move the reference.
	if _, err := dana.DefineType(ctx, "orders", "invoice", client.TypeDefinition{
		Schema:     json.RawMessage(`{"type":"object"}`),
		Operations: map[string]contract.OpDef{"order.update": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge}},
	}); err != nil {
		t.Fatalf("define type: %v", err)
	}
	if _, err := dana.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"customer":"cust-1"}`)); err != nil {
		t.Fatalf("create invoice-1: %v", err)
	}
	if _, err := dana.CreateThing(ctx, "orders", "invoice.invoice-2", json.RawMessage(`{"customer":"cust-1"}`)); err != nil {
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
	if _, err := dana.Append(ctx, "orders", "invoice.invoice-1", "order.update", []byte(`{"customer":"cust-2"}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		in, err := dana.GraphNeighbors(ctx, "orders", "refs", client.GraphQueryRequest{Thing: "cust-2", Direction: "in"})
		if err == nil && in.Total == 1 && in.Edges[0].From == "invoice.invoice-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("live tail never rewired the edge: %v %+v", err, in)
		}
		time.Sleep(50 * time.Millisecond)
	}
	walk, err := dana.GraphWalk(ctx, "orders", "refs", client.GraphQueryRequest{Thing: "invoice.invoice-1", Depth: 1})
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

// TestDeclaredSemanticServes is the semantic kind's floor (0016): with a
// provider configured, a declaration becomes a served meaning index; the
// reply ranks by best chunk. Without a provider, the same declaration
// stays honestly unschedulable — recorded, no responder, no pretense.
func TestDeclaredSemanticServes(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
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
				0.1,
			}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer provider.Close()

	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	f, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1, Embedding: &semantic.ProviderConfig{BaseURL: provider.URL, Model: "test-embed"}})
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
	if _, err := dana.CreateThing(ctx, "orders", "invoice.invoice-1", json.RawMessage(`{"title":"quantum widget order"}`)); err != nil {
		t.Fatalf("create invoice-1: %v", err)
	}
	if _, err := dana.CreateThing(ctx, "orders", "invoice.invoice-2", json.RawMessage(`{"title":"gadget shipment"}`)); err != nil {
		t.Fatalf("create invoice-2: %v", err)
	}
	if _, err := dana.DeclareIndex(ctx, "orders", "meaning", "semantic", nil); err != nil {
		t.Fatalf("declare semantic index: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := dana.QuerySemantic(ctx, "orders", "meaning", "widget", 0, 0)
		if err == nil && len(resp.Hits) > 0 && resp.Hits[0].Thing == "invoice.invoice-1" && resp.Unembedded == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("declared semantic index never served: %v %+v", err, resp)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSemanticWithoutProviderIsUnschedulable: the executor does not bid
// for a kind its install cannot carry, so the declaration is recorded
// and the workload waits — no responder, no failing placements, and the
// moment a provisioned executor exists the record is already there.
func TestSemanticWithoutProviderIsUnschedulable(t *testing.T) {
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
	if _, err := dana.DeclareIndex(ctx, "orders", "meaning", "semantic", nil); err != nil {
		t.Fatalf("declare semantic index: %v", err)
	}
	// A few scan ticks pass; the declaration stands, nothing answers.
	time.Sleep(2 * time.Second)
	if _, err := dana.QuerySemantic(ctx, "orders", "meaning", "widget", 0, 0); err == nil {
		t.Fatal("an unprovisioned fleet answered a semantic query")
	}
}

// waitEvicted polls until the server closes the connection: revocation and
// rekey both evict actively, and the client parks in reconnect.
func waitEvicted(t *testing.T, c *client.Client, who string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for c.Conn().IsConnected() {
		if time.Now().After(deadline) {
			t.Fatalf("%s still connected: eviction did not land", who)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestMemberLifecycle is chronicle-15's incident run as a test: a second
// principal joins a running tenant, their credential leaks, the revoke
// kills it mid-flight — and when everything must be assumed burned, the
// rekey replaces the member-issuing key while the tenant's node rides
// through on the service user.
func TestMemberLifecycle(t *testing.T) {
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
	defer ctrl.Close()
	minted, err := ctrl.MintTenant(ctx, "acme", "dana")
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

	// A second principal joins the running tenant — the verb that did not
	// exist when the only options were re-mint or destroy.
	added, err := ctrl.AddMember(ctx, "acme", "erin", "", 0)
	if err != nil {
		t.Fatalf("add member: %v", err)
	}
	if added.Role != contract.RoleWriter {
		t.Fatalf("default role = %q, want writer", added.Role)
	}
	erin, err := client.Connect(f.URL, added.Creds)
	if err != nil {
		t.Fatalf("connect erin: %v", err)
	}
	defer erin.Close()
	if erin.Author() != "erin" {
		t.Fatalf("author from creds = %q", erin.Author())
	}
	// The data plane is open to a writer; the admin line holds.
	if _, err := erin.CreateThing(ctx, "orders", "ticket-1", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("writer create thing: %v", err)
	}
	if _, err := erin.CreateLog(ctx, "rogue", ""); err == nil {
		t.Fatal("a writer created a log: the role gate is broken")
	}

	// The add refuses what it must: a duplicate, a made-up role, a tenant
	// that does not exist.
	var serr *client.ServiceError
	if _, err := ctrl.AddMember(ctx, "acme", "erin", "", 0); !errors.As(err, &serr) || serr.Code != "member-exists" {
		t.Fatalf("duplicate add: %v", err)
	}
	if _, err := ctrl.AddMember(ctx, "acme", "frank", "sudo", 0); !errors.As(err, &serr) || serr.Code != "bad-role" {
		t.Fatalf("bad role: %v", err)
	}
	if _, err := ctrl.AddMember(ctx, "nosuch", "erin", "", 0); !errors.As(err, &serr) || serr.Code != "no-such-tenant" {
		t.Fatalf("no such tenant: %v", err)
	}

	// The leak: revoke erin. The live connection dies, a fresh dial is
	// refused, and the neighbor never blinks.
	revoked, err := ctrl.RevokeMember(ctx, "acme", "erin")
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if revoked.PublicKey == "" {
		t.Fatalf("revoke response incomplete: %+v", revoked)
	}
	waitEvicted(t, erin, "erin")
	if c, err := client.Connect(f.URL, added.Creds); err == nil {
		c.Close()
		t.Fatal("revoked creds reconnected")
	}
	if _, err := ctrl.RevokeMember(ctx, "acme", "erin"); !errors.As(err, &serr) || serr.Code != "not-a-member" {
		t.Fatalf("second revoke: %v", err)
	}
	if !dana.Conn().IsConnected() {
		t.Fatal("revoking erin evicted dana")
	}

	// Re-adding the same principal works — the registry entry is gone, the
	// durable identity remains, the new user key is fresh.
	readded, err := ctrl.AddMember(ctx, "acme", "erin", contract.RoleReader, 0)
	if err != nil {
		t.Fatalf("re-add after revoke: %v", err)
	}
	erin2, err := client.Connect(f.URL, readded.Creds)
	if err != nil {
		t.Fatalf("connect re-added erin: %v", err)
	}
	defer erin2.Close()

	// The kill switch: everything member-held is burned at once. The node
	// rides through on the service user — the log create after proves the
	// tenant never stopped serving.
	rekeyed, err := ctrl.RekeyMembers(ctx, "acme")
	if err != nil {
		t.Fatalf("rekey: %v", err)
	}
	if len(rekeyed.Members) != 2 {
		t.Fatalf("rekey re-issued %d members, want 2", len(rekeyed.Members))
	}
	waitEvicted(t, dana, "dana")
	waitEvicted(t, erin2, "erin")
	if c, err := client.Connect(f.URL, minted.AdminCreds); err == nil {
		c.Close()
		t.Fatal("pre-rekey admin creds survived the rekey")
	}
	byID := map[string]client.MemberAddResponse{}
	for _, m := range rekeyed.Members {
		byID[m.Principal] = m
	}
	if byID["dana"].Role != contract.RoleAdmin || byID["erin"].Role != contract.RoleReader {
		t.Fatalf("roles did not survive the rekey: %+v", rekeyed.Members)
	}
	dana2, err := client.Connect(f.URL, byID["dana"].Creds)
	if err != nil {
		t.Fatalf("connect re-issued admin: %v", err)
	}
	defer dana2.Close()
	if _, err := dana2.CreateLog(ctx, "after-rekey", ""); err != nil {
		t.Fatalf("create log after rekey: %v", err)
	}
}

// TestOperatorRotationRoundTrip: the trust root rotates offline, and the
// fleet that boots after it serves every credential minted before — the
// operator identity persists exactly so this ceremony can exist.
func TestOperatorRotationRoundTrip(t *testing.T) {
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
	if _, err := dana.CreateLog(ctx, "orders", ""); err != nil {
		dana.Close()
		t.Fatalf("create log: %v", err)
	}
	dana.Close()
	f.Stop()
	stopped = true

	var out bytes.Buffer
	if err := fleet.RotateSigningKey(ctx, []string{"--dir", dir}, &out); err != nil {
		t.Fatalf("rotate: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "operator signing key rotated") {
		t.Fatalf("rotate output: %s", out.String())
	}

	// The fleet boots trusting only the new key; the tenant node comes
	// back, pre-rotation creds still work, and a fresh mint lands.
	f2, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up after rotation: %v", err)
	}
	defer f2.Stop()

	dana2, err := client.Connect(f2.URL, minted.AdminCreds)
	if err != nil {
		t.Fatalf("pre-rotation admin creds refused: %v", err)
	}
	defer dana2.Close()
	if _, err := dana2.CreateLog(ctx, "after-rotation", ""); err != nil {
		t.Fatalf("create log after rotation: %v", err)
	}
	ctrl2, err := client.ConnectControlCreds(f2.URL, ctrlCreds)
	if err != nil {
		t.Fatalf("connect control after rotation: %v", err)
	}
	defer ctrl2.Close()
	if _, err := ctrl2.MintTenant(ctx, "beta", ""); err != nil {
		t.Fatalf("mint under the rotated key: %v", err)
	}
}

// TestUpFleetUsersAreFenced: `up` walks design 10's first boot, so its own
// members — the workload service, the embedded executor, the CLI — hold
// fleet-template users that cannot reach the bucket, the root keeps no
// working key once sealed, and a restart reuses the bundles it issued.
func TestUpFleetUsersAreFenced(t *testing.T) {
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

	bundles := map[string][]byte{}
	for _, name := range []string{"workloads", fleet.LocalExecutorID, "cli"} {
		b, err := mint.ReadBundle(filepath.Join(dir, "bundles", name))
		if err != nil || b.IsControlInstance() {
			t.Fatalf("bundle %s: %+v, %v", name, b, err)
		}
		if issued, err := mint.InstanceOf(b.ControlCreds); err != nil || issued != name {
			t.Fatalf("bundle %s was issued for %q, %v", name, issued, err)
		}
		bundles[name] = b.ControlCreds
		assertFenced(t, f.URL, b.ControlCreds, name)
	}
	// The roles hold: the embedded executor's credential cannot mint.
	if ec, err := client.ConnectControlCreds(f.URL, bundles[fleet.LocalExecutorID]); err == nil {
		mctx, mcancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := ec.MintTenant(mctx, "rogue", "")
		mcancel()
		ec.Close()
		if err == nil {
			t.Fatal("the executor's credential minted a tenant")
		}
	}
	first, err := mint.ReadBundle(filepath.Join(dir, "bundles", "instance-1"))
	if err != nil || !first.IsControlInstance() {
		t.Fatalf("instance-1's bundle: %v", err)
	}
	inc, err := mint.ConnectCreds(f.URL, first.ControlCreds, "instance-1-reads")
	if err != nil {
		t.Fatalf("instance-1 connects: %v", err)
	}
	if _, err := mint.OpenCustody(ctx, inc); err != nil {
		t.Fatalf("the control instance cannot open the bucket: %v", err)
	}
	inc.Close()

	// The dev dir keeps what the offline root keeps: identity, node keys,
	// bundles, exports — no working key, no bootstrap user.
	for _, file := range []string{"operator-signing.nk", "sys-account.nk", "control-account.nk", "bridge.creds"} {
		if _, err := os.Stat(filepath.Join(dir, file)); !os.IsNotExist(err) {
			t.Fatalf("up left %s in the dev dir (%v)", file, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "operator.nk")); err != nil {
		t.Fatalf("up took the operator identity: %v", err)
	}

	// A restart reuses the bundles: the same users, still fenced, and the
	// CLI's still mints.
	f.Stop()
	stopped = true
	f2, err := fleet.Up(ctx, fleet.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up again: %v", err)
	}
	defer f2.Stop()
	for name, creds := range bundles {
		b, err := mint.ReadBundle(filepath.Join(dir, "bundles", name))
		if err != nil || !bytes.Equal(b.ControlCreds, creds) {
			t.Fatalf("bundle %s was re-issued on restart (%v)", name, err)
		}
	}
	ctrl, err := client.ConnectControlCreds(f2.URL, bundles["cli"])
	if err != nil {
		t.Fatalf("cli bundle connects: %v", err)
	}
	defer ctrl.Close()
	if _, err := ctrl.MintTenant(ctx, "acme", "dana"); err != nil {
		t.Fatalf("mint through the cli bundle: %v", err)
	}
}
