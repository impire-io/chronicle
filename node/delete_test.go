package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/node"
)

// metaKeysWith lists META keys under a prefix — the assertion that nothing
// of a store remains.
func metaKeysWith(ctx context.Context, t *testing.T, meta jetstream.KeyValue, prefix string) []string {
	t.Helper()
	keys, err := meta.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		t.Fatalf("keys: %v", err)
	}
	var out []string
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

// TestStoreDelete: decision 0062 point 2. The delete is total — stream,
// state bucket, index declarations, type records and the config key all
// gone — a writer may not speak it, a store that does not exist is
// refused by name, and the name may be created again.
func TestStoreDelete(t *testing.T) {
	nc, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := alice.CreateStore(ctx, "orders", "", client.WithHistory(contract.HistoryFull)); err != nil {
		t.Fatalf("create store: %v", err)
	}
	if _, err := alice.DefineType(ctx, "orders", "invoice", client.TypeDefinition{Schema: json.RawMessage(`{"type":"object"}`), Operations: map[string]contract.OpDef{"create": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge}}}); err != nil {
		t.Fatalf("define type: %v", err)
	}
	if _, err := alice.DeclareIndex(ctx, "orders", "text", contract.IndexKindSearch, nil); err != nil {
		t.Fatalf("declare index: %v", err)
	}
	if _, err := alice.Create(ctx, "orders", "invoice/inv-1", "create", []byte(`{"total":120}`)); err != nil {
		t.Fatalf("create instance: %v", err)
	}
	waitStateAt(ctx, t, alice, "orders", "invoice/inv-1", 1)

	// A writer may not delete a store; a store that does not exist is
	// refused by name.
	wally, err := client.Wrap(alice.Conn(), "wally")
	if err != nil {
		t.Fatal(err)
	}
	_, err = wally.DeleteStore(ctx, "orders")
	refused(t, err, "forbidden")
	_, err = alice.DeleteStore(ctx, "ghost")
	refused(t, err, "no-such-store")

	// The full history policy binds the stream's own API paths, not the
	// deletion of the stream.
	resp, err := alice.DeleteStore(ctx, "orders")
	if err != nil || !resp.Deleted {
		t.Fatalf("delete store: %v %+v", err, resp)
	}
	if _, err := js.Stream(ctx, contract.StreamName("orders")); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("stream after delete: %v", err)
	}
	if _, err := js.KeyValue(ctx, contract.StateBucket("orders")); err == nil {
		t.Fatalf("state bucket survived the delete")
	}
	for _, prefix := range []string{contract.MetaStoreConfig("orders"), contract.MetaStoreType("orders", ""), contract.MetaIndex("orders", "")} {
		if left := metaKeysWith(ctx, t, meta, prefix); len(left) != 0 {
			t.Fatalf("META keys left under %s: %v", prefix, left)
		}
	}
	for name, err := range alice.ListStores(ctx) {
		if err != nil {
			t.Fatalf("list stores: %v", err)
		}
		if name == "orders" {
			t.Fatalf("store listed after delete")
		}
	}
	_, err = alice.DeleteStore(ctx, "orders")
	refused(t, err, "no-such-store")
	_, err = alice.DefineType(ctx, "orders", "invoice", client.TypeDefinition{Schema: json.RawMessage(`{"type":"object"}`)})
	refused(t, err, "no-such-store")

	// The name is free: a new store of it is a new store, empty.
	if _, err := alice.CreateStore(ctx, "orders", ""); err != nil {
		t.Fatalf("create store again: %v", err)
	}
	types := collect(t, alice.ListTypes(ctx, "orders"))
	if len(types) != 0 {
		t.Fatalf("the new store carries types: %v", types)
	}
	if _, err := alice.DeleteStore(ctx, "orders"); err != nil {
		t.Fatalf("delete again: %v", err)
	}
}

// TestStoreDeleteFinishesFromAnyStep: the deleting status is the claim
// (point 2) — every other verb on the store refuses with it, a create of
// the name refuses with it, a second delete finishes the job, and a node
// that boots on a store left deleting finishes it instead of folding.
func TestStoreDeleteFinishesFromAnyStep(t *testing.T) {
	url := natstest.StartJetStream(t)
	nc, alice := startNodeOn(t, url, nil, node.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		t.Fatal(err)
	}
	markDeleting := func(store string) {
		t.Helper()
		entry, err := meta.Get(ctx, contract.MetaStoreConfig(store))
		if err != nil {
			t.Fatal(err)
		}
		var cfg contract.StoreConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
			t.Fatal(err)
		}
		cfg.Status = contract.StoreStatusDeleting
		value, _ := json.Marshal(cfg)
		if _, err := meta.Update(ctx, contract.MetaStoreConfig(store), value, entry.Revision()); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := alice.CreateStore(ctx, "orders", ""); err != nil {
		t.Fatalf("create store: %v", err)
	}
	if _, err := alice.CreateFromSnapshot(ctx, "orders", "ticket-1", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("create instance: %v", err)
	}
	// A crashed first run left the claim and nothing else.
	markDeleting("orders")
	_, err = alice.DefineType(ctx, "orders", "invoice", client.TypeDefinition{Schema: json.RawMessage(`{"type":"object"}`)})
	refused(t, err, "store-deleting")
	_, err = alice.DeclareIndex(ctx, "orders", "text", contract.IndexKindSearch, nil)
	refused(t, err, "store-deleting")
	_, err = alice.Snapshot(ctx, "orders", "ticket-1")
	refused(t, err, "store-deleting")
	_, err = alice.CreateStore(ctx, "orders", "")
	refused(t, err, "store-deleting")
	cfg, err := alice.GetStore(ctx, "orders")
	if err != nil || cfg.Status != contract.StoreStatusDeleting {
		t.Fatalf("store get while deleting: %v %+v", err, cfg)
	}
	// The second run finishes.
	if _, err := alice.DeleteStore(ctx, "orders"); err != nil {
		t.Fatalf("finish delete: %v", err)
	}
	if _, err := js.Stream(ctx, contract.StreamName("orders")); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("stream after delete: %v", err)
	}
	if left := metaKeysWith(ctx, t, meta, "log.orders."); len(left) != 0 {
		t.Fatalf("META keys left: %v", left)
	}

	// A replica that boots on a store left deleting — here with its stream
	// already gone, the worst case for a fold — finishes the delete.
	if _, err := alice.CreateStore(ctx, "scratch", ""); err != nil {
		t.Fatalf("create store: %v", err)
	}
	markDeleting("scratch")
	if err := js.DeleteStream(ctx, contract.StreamName("scratch")); err != nil {
		t.Fatal(err)
	}
	_, second := startNodeOn(t, url, nil, node.Config{})
	if left := metaKeysWith(ctx, t, meta, "log.scratch."); len(left) != 0 {
		t.Fatalf("META keys left after boot: %v", left)
	}
	if _, err := js.KeyValue(ctx, contract.StateBucket("scratch")); err == nil {
		t.Fatalf("state bucket survived the boot's finish")
	}
	if _, err := second.CreateStore(ctx, "scratch", ""); err != nil {
		t.Fatalf("create after the boot finished the delete: %v", err)
	}
}

// TestTypeDelete: decision 0062 point 4. The type's instances are untyped
// on the re-fold — history kept, no state, absent from the listing — and
// defining the type again restores them; an undefined type is refused by
// name, and a sibling type is untouched.
func TestTypeDelete(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := alice.CreateStore(ctx, "orders", ""); err != nil {
		t.Fatalf("create store: %v", err)
	}
	invoice := client.TypeDefinition{Schema: json.RawMessage(`{"type":"object"}`), Operations: map[string]contract.OpDef{"create": {Schema: json.RawMessage(`{"type":"object"}`), Effect: contract.EffectMerge}}}
	for _, name := range []string{"invoice", "note"} {
		if _, err := alice.DefineType(ctx, "orders", name, invoice); err != nil {
			t.Fatalf("define %s: %v", name, err)
		}
	}
	if _, err := alice.Create(ctx, "orders", "invoice/inv-1", "create", []byte(`{"total":120}`)); err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := alice.Create(ctx, "orders", "note/n-1", "create", []byte(`{"text":"x"}`)); err != nil {
		t.Fatalf("create note: %v", err)
	}
	waitStateAt(ctx, t, alice, "orders", "invoice/inv-1", 1)
	waitStateAt(ctx, t, alice, "orders", "note/n-1", 2)

	_, err := alice.DeleteType(ctx, "orders", "ghost")
	refused(t, err, "no-such-type")
	wally, err := client.Wrap(alice.Conn(), "wally")
	if err != nil {
		t.Fatal(err)
	}
	_, err = wally.DeleteType(ctx, "orders", "invoice")
	refused(t, err, "forbidden")

	if _, err := alice.DeleteType(ctx, "orders", "invoice"); err != nil {
		t.Fatalf("delete type: %v", err)
	}
	types := collect(t, alice.ListTypes(ctx, "orders"))
	if len(types) != 1 || types[0] != "note" {
		t.Fatalf("types after delete: %v", types)
	}
	listed := func() []string {
		var paths []string
		for info, err := range alice.ListInstances(ctx, "orders") {
			if err != nil {
				t.Fatal(err)
			}
			paths = append(paths, info.Path)
		}
		return paths
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		paths := listed()
		if len(paths) == 1 && paths[0] == "note/n-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("instances after type delete: %v", paths)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The history is whole.
	ops := collect(t, alice.History(ctx, "orders", "invoice/inv-1"))
	if len(ops) != 1 || ops[0].Type != "create" {
		t.Fatalf("history after type delete: %+v", ops)
	}
	// A create of the deleted type is a create of an undefined type.
	if _, err := alice.Create(ctx, "orders", "invoice/inv-2", "create", []byte(`{}`)); err == nil {
		t.Fatalf("a create of a deleted type must be refused")
	}

	// Defining the type again restores its instances on the re-fold.
	if _, err := alice.DefineType(ctx, "orders", "invoice", invoice); err != nil {
		t.Fatalf("define again: %v", err)
	}
	sv := waitStateAt(ctx, t, alice, "orders", "invoice/inv-1", 1)
	if !strings.Contains(string(sv.State), `"total":120`) {
		t.Fatalf("state after the type returned: %s", sv.State)
	}
}

// TestSetRole: decision 0062 point 5. The role changes in place and the
// next request is judged by it; a member and a service account alike;
// the role already held is answered; the only admin keeps the role, under
// set-role and under revoke; a stranger and a role outside the vocabulary
// are refused by name.
func TestSetRole(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := alice.CreateStore(ctx, "orders", ""); err != nil {
		t.Fatalf("create store: %v", err)
	}
	if _, err := alice.AddMember(ctx, "erin", contract.RoleReader, client.WithPublicKey("UERIN")); err != nil {
		t.Fatalf("add member: %v", err)
	}
	if _, err := alice.AddMember(ctx, "billing-svc", contract.RoleReader, client.AsServiceAccount()); err != nil {
		t.Fatalf("add service account: %v", err)
	}
	erin, err := client.Wrap(alice.Conn(), "erin")
	if err != nil {
		t.Fatal(err)
	}
	_, err = erin.CreateStore(ctx, "rogue", "")
	refused(t, err, "forbidden")

	// Seeded by the harness: alice admin, rita reader, wally writer. Alice
	// is the only admin: she keeps the role, and cannot be revoked.
	_, err = alice.SetRole(ctx, "alice", contract.RoleReader)
	refused(t, err, "sole-admin")
	_, err = alice.RevokeMember(ctx, "alice")
	refused(t, err, "sole-admin")
	// The role already held is answered.
	if resp, err := alice.SetRole(ctx, "alice", contract.RoleAdmin); err != nil || resp.Role != contract.RoleAdmin {
		t.Fatalf("set-role to the same role: %v %+v", err, resp)
	}

	// Promoted, erin governs from the next request; the record keeps its
	// key and kind.
	resp, err := alice.SetRole(ctx, "erin", contract.RoleAdmin)
	if err != nil || resp.Member != "erin" || resp.Role != contract.RoleAdmin {
		t.Fatalf("set-role: %v %+v", err, resp)
	}
	if _, err := erin.CreateStore(ctx, "rogue", ""); err != nil {
		t.Fatalf("erin as admin: %v", err)
	}
	for _, m := range collect(t, alice.ListMembers(ctx)) {
		if m.Name == "erin" && (m.Role != contract.RoleAdmin || m.PublicKey != "UERIN") {
			t.Fatalf("erin's record: %+v", m)
		}
	}
	// With a second admin, alice may step down; then erin is the only one.
	if _, err := alice.SetRole(ctx, "alice", contract.RoleWriter); err != nil {
		t.Fatalf("alice steps down: %v", err)
	}
	_, err = alice.SetRole(ctx, "erin", contract.RoleReader)
	refused(t, err, "forbidden")
	_, err = erin.SetRole(ctx, "erin", contract.RoleReader)
	refused(t, err, "sole-admin")
	if _, err := erin.SetRole(ctx, "alice", contract.RoleAdmin); err != nil {
		t.Fatalf("erin restores alice: %v", err)
	}

	// A service account is changed the same way.
	if _, err := alice.SetRole(ctx, "billing-svc", contract.RoleWriter); err != nil {
		t.Fatalf("service account set-role: %v", err)
	}
	for _, m := range collect(t, alice.ListMembers(ctx)) {
		if m.Name == "billing-svc" && (m.Role != contract.RoleWriter || m.Kind != contract.PrincipalKindService) {
			t.Fatalf("billing-svc's record: %+v", m)
		}
	}

	// The gates.
	_, err = alice.SetRole(ctx, "nobody", contract.RoleAdmin)
	refused(t, err, "not-a-member")
	_, err = alice.SetRole(ctx, "erin", "owner")
	refused(t, err, "bad-role")
	_, err = alice.SetRole(ctx, "Bad.Name", contract.RoleAdmin)
	refused(t, err, "bad-principal-name")
	wally, err := client.Wrap(alice.Conn(), "wally")
	if err != nil {
		t.Fatal(err)
	}
	_, err = wally.SetRole(ctx, "erin", contract.RoleReader)
	refused(t, err, "forbidden")
}
