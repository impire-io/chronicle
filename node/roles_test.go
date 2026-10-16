package node_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/node"
	"github.com/impire-io/chronicle/registry"
)

// TestRolesByCallout: a node started with the callout resolver (decision
// 0061) asks the environment's responder on CHRON.ROLE.<principal> for
// every caller's role and reads the registry not at all — alice, the
// registry's seeded admin, is whatever the responder says; zoe, whom the
// registry never heard of, is its admin; a stranger to the responder is
// not a member; the member verbs are refused because membership is
// managed where the roles live; the service principal is the operator
// without a question asked; and a responder that does not answer refuses
// the request, never answered from META.
func TestRolesByCallout(t *testing.T) {
	url := natstest.StartJetStream(t)
	env, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(env.Close)
	roles := map[string]string{"alice": contract.RoleReader, "zoe": contract.RoleAdmin}
	asked := make(chan string, 64)
	sub, err := env.Subscribe(contract.RoleCalloutFilter, func(m *nats.Msg) {
		principal := m.Subject[strings.LastIndexByte(m.Subject, '.')+1:]
		asked <- principal
		reply, _ := json.Marshal(contract.RoleReply{Role: roles[principal]})
		_ = m.Respond(reply)
	})
	if err != nil {
		t.Fatalf("responder: %v", err)
	}
	drain := func() {
		for {
			select {
			case <-asked:
			default:
				return
			}
		}
	}

	_, alice := startNodeOn(t, url, nil, node.Config{Roles: registry.Callout(env, time.Second)})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The registry says alice is admin; the responder says reader, and
	// the responder is what the node asks.
	_, err = alice.CreateStore(ctx, "orders", "")
	refused(t, err, contract.CodeForbidden)
	if got := <-asked; got != "alice" {
		t.Fatalf("asked about %q, want alice", got)
	}

	// Zoe is nobody to the registry and the responder's admin.
	zoe, err := client.Wrap(alice.Conn(), "zoe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zoe.CreateStore(ctx, "orders", ""); err != nil {
		t.Fatalf("zoe, the responder's admin: %v", err)
	}
	if got := <-asked; got != "zoe" {
		t.Fatalf("asked about %q, want zoe", got)
	}

	// A stranger to the responder is not a member.
	nobody, err := client.Wrap(alice.Conn(), "nobody")
	if err != nil {
		t.Fatal(err)
	}
	_, err = nobody.CreateStore(ctx, "rogue", "")
	refused(t, err, contract.CodeForbidden)
	if err == nil || !strings.Contains(err.Error(), "not a member") {
		t.Fatalf("nobody: %v", err)
	}

	// Membership is managed where the roles live: the member verbs have
	// nothing to write, and the responder is not even asked.
	drain()
	_, err = zoe.AddMember(ctx, "erin", contract.RoleWriter)
	refused(t, err, contract.CodeMembershipElsewhere)
	_, err = zoe.RevokeMember(ctx, "alice")
	refused(t, err, contract.CodeMembershipElsewhere)
	select {
	case p := <-asked:
		t.Fatalf("the responder was asked about %q for a member verb", p)
	default:
	}

	// The service principal is the operator, asked of nobody.
	service, err := client.Wrap(alice.Conn(), contract.ServicePrincipal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateStore(ctx, "ops", ""); err != nil {
		t.Fatalf("the service principal: %v", err)
	}
	select {
	case p := <-asked:
		t.Fatalf("the responder was asked about %q for the service principal", p)
	default:
	}

	// Silence fails closed.
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	_, err = zoe.CreateStore(ctx, "later", "")
	refused(t, err, contract.CodeRoleSourceUnavailable)
}
