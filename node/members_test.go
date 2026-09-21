package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

func refused(t *testing.T, err error, code string) {
	t.Helper()
	var serr *client.ServiceError
	if !errors.As(err, &serr) || serr.Code != code {
		t.Fatalf("want refusal %q, got %v", code, err)
	}
}

// allowed asserts a verb was not refused by the role check — the verb may
// still decline on its own grounds (a rollup with nothing to compact).
func allowed(t *testing.T, err error) {
	t.Helper()
	var serr *client.ServiceError
	if errors.As(err, &serr) && serr.Code == "forbidden" {
		t.Fatalf("refused: %v", err)
	}
}

// TestMemberVerbs: the node writes the registry (11-the-two-forms.md §
// membership, without custody). An admin adds and revokes; the role a
// membership carries is what the API verbs enforce from the next request
// on — appends are direct publishes and the wire's business (0008), so
// the probes are verbs: rollup takes admin or writer, log create admin;
// the record is the dedup gate; the principal record survives a revoke;
// the tenant's own service principal holds every role without an entry.
func TestMemberVerbs(t *testing.T) {
	_, alice := startNode(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := alice.CreateLog(ctx, "orders", ""); err != nil {
		t.Fatalf("create log: %v", err)
	}

	// Add: default role writer; the new member writes but cannot govern.
	added, err := alice.AddMember(ctx, "erin", "", client.WithPublicKey("UERIN"))
	if err != nil {
		t.Fatalf("add member: %v", err)
	}
	if added.Member != "erin" || added.Role != contract.RoleWriter {
		t.Fatalf("added = %+v", added)
	}
	erin, err := client.Wrap(alice.Conn(), "erin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alice.CreateThing(ctx, "orders", "ticket-1", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("create thing: %v", err)
	}
	_, err = erin.RollupThing(ctx, "orders", "ticket-1")
	allowed(t, err)
	_, err = erin.CreateLog(ctx, "rogue", "")
	refused(t, err, "forbidden")
	_, err = erin.AddMember(ctx, "mallory", contract.RoleAdmin)
	refused(t, err, "forbidden")

	// The registry lists what the verb recorded.
	members := collect(t, erin.ListMembers(ctx))
	byName := map[string]client.MemberInfo{}
	for _, m := range members {
		byName[m.Name] = m
	}
	if byName["erin"].PublicKey != "UERIN" || byName["erin"].Role != contract.RoleWriter || byName["alice"].Role != contract.RoleAdmin {
		t.Fatalf("members = %+v", members)
	}

	// The gates: a second add of the same principal, a role outside the
	// vocabulary, a name outside the grammar, the reserved service name.
	_, err = alice.AddMember(ctx, "erin", contract.RoleReader)
	refused(t, err, "member-exists")
	_, err = alice.AddMember(ctx, "frank", "owner")
	refused(t, err, "bad-role")
	_, err = alice.AddMember(ctx, "Frank.Two", "")
	refused(t, err, "bad-principal-name")
	_, err = alice.AddMember(ctx, contract.ServicePrincipal, "")
	refused(t, err, "bad-principal-name")

	// Revoke: the record goes, the reply names the key, the principal
	// can act no more, and revoking again is not-a-member.
	revoked, err := alice.RevokeMember(ctx, "erin")
	if err != nil {
		t.Fatalf("revoke member: %v", err)
	}
	if revoked.PublicKey != "UERIN" {
		t.Fatalf("revoked = %+v", revoked)
	}
	_, err = erin.RollupThing(ctx, "orders", "ticket-1")
	refused(t, err, "forbidden")
	_, err = alice.RevokeMember(ctx, "erin")
	refused(t, err, "not-a-member")

	// Re-added, erin is the same principal, now a reader.
	if _, err := alice.AddMember(ctx, "erin", contract.RoleReader); err != nil {
		t.Fatalf("re-add member: %v", err)
	}
	_, err = erin.RollupThing(ctx, "orders", "ticket-1")
	refused(t, err, "forbidden")

	// The tenant's own service acts as the operator: every role, no
	// registry entry — the managed service composing issuance with the
	// verb.
	service, err := client.Wrap(alice.Conn(), contract.ServicePrincipal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.AddMember(ctx, "gus", contract.RoleAdmin, client.WithGithubID(42)); err != nil {
		t.Fatalf("service add member: %v", err)
	}
	members = collect(t, alice.ListMembers(ctx))
	for _, m := range members {
		if m.Name == contract.ServicePrincipal {
			t.Fatal("the service principal is in the registry")
		}
		if m.Name == "gus" && m.GithubID != 42 {
			t.Fatalf("gus = %+v", m)
		}
	}
}
