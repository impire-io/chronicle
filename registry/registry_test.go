package registry_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/registry"
)

func connect(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(natstest.StartJetStream(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// TestMetaResolver: the default reads the identity slice of META as
// decision 0007 rules — a membership's role, not-a-member for a principal
// the slice does not hold.
func TestMetaResolver(t *testing.T) {
	nc := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	meta, err := registry.Seed(ctx, js, "alice", "UALICE")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	r := registry.Meta(meta)
	role, err := r.Role(ctx, "alice")
	if err != nil || role != contract.RoleAdmin {
		t.Fatalf("alice: %q, %v", role, err)
	}
	if _, err := r.Role(ctx, "nobody"); !errors.Is(err, registry.ErrNotAMember) {
		t.Fatalf("nobody: want not a member, got %v", err)
	}
}

// TestCalloutResolver: the callout asks CHRON.ROLE.<principal> and takes
// the responder's answer — a role, no role for a non-member — and fails
// closed on a role outside the vocabulary, a reply that does not decode,
// a responder that does not answer in time, and no responder at all.
func TestCalloutResolver(t *testing.T) {
	nc := connect(t)
	ctx := context.Background()
	answers := map[string]string{
		"alice": `{"role":"admin"}`,
		"erin":  `{"role":"writer"}`,
		"zed":   `{}`,
		"boss":  `{"role":"owner"}`,
		"junk":  `nope`,
	}
	sub, err := nc.Subscribe(contract.RoleCalloutFilter, func(m *nats.Msg) {
		principal := m.Subject[strings.LastIndexByte(m.Subject, '.')+1:]
		if principal == "slow" {
			return // never answers
		}
		_ = m.Respond([]byte(answers[principal]))
	})
	if err != nil {
		t.Fatalf("responder: %v", err)
	}
	if contract.RoleSubject("alice") != "CHRON.ROLE.alice" {
		t.Fatalf("subject %q", contract.RoleSubject("alice"))
	}

	r := registry.Callout(nc, 300*time.Millisecond)
	for principal, want := range map[string]string{"alice": contract.RoleAdmin, "erin": contract.RoleWriter} {
		role, err := r.Role(ctx, principal)
		if err != nil || role != want {
			t.Fatalf("%s: %q, %v", principal, role, err)
		}
	}
	if _, err := r.Role(ctx, "zed"); !errors.Is(err, registry.ErrNotAMember) {
		t.Fatalf("zed: want not a member, got %v", err)
	}
	for _, principal := range []string{"boss", "junk"} {
		if _, err := r.Role(ctx, principal); !errors.Is(err, registry.ErrRoleSourceUnavailable) {
			t.Fatalf("%s: want the source unavailable, got %v", principal, err)
		}
	}
	start := time.Now()
	if _, err := r.Role(ctx, "slow"); !errors.Is(err, registry.ErrRoleSourceUnavailable) {
		t.Fatalf("slow: want the source unavailable, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("slow: the bound did not hold, waited %v", elapsed)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Role(ctx, "alice"); !errors.Is(err, registry.ErrRoleSourceUnavailable) {
		t.Fatalf("no responder: want the source unavailable, got %v", err)
	}
}

// stub is an in-process resolver — the embedder's seam (decision 0061
// point 5) — that answers one thing and counts the questions.
type stub struct {
	role  string
	err   error
	asked int
}

func (s *stub) Role(context.Context, string) (string, error) {
	s.asked++
	return s.role, s.err
}

// TestRequire: the shared check admits by role, refuses by role and by
// absence, never asks about the service principal, refuses an empty
// principal, and each refusal maps to its wire code.
func TestRequire(t *testing.T) {
	ctx := context.Background()
	s := &stub{role: contract.RoleWriter}
	if err := registry.Require(ctx, s, "erin", contract.RoleAdmin, contract.RoleWriter); err != nil {
		t.Fatalf("writer among admin, writer: %v", err)
	}
	err := registry.Require(ctx, s, "erin", contract.RoleAdmin)
	if err == nil || !strings.Contains(err.Error(), `"writer"`) {
		t.Fatalf("writer where admin required: %v", err)
	}
	if registry.Code(err) != contract.CodeForbidden {
		t.Fatalf("code %q", registry.Code(err))
	}
	if err := registry.Require(ctx, s, contract.ServicePrincipal, contract.RoleAdmin); err != nil {
		t.Fatalf("the service principal: %v", err)
	}
	if s.asked != 2 {
		t.Fatalf("asked %d times; the service principal is asked of nobody", s.asked)
	}
	if err := registry.Require(ctx, s, "", contract.RoleAdmin); err == nil {
		t.Fatal("an empty principal passed")
	}

	absent := &stub{err: registry.ErrNotAMember}
	err = registry.Require(ctx, absent, "ghost", contract.RoleReader)
	if !errors.Is(err, registry.ErrNotAMember) || !strings.Contains(err.Error(), `"ghost"`) {
		t.Fatalf("absent: %v", err)
	}
	if registry.Code(err) != contract.CodeForbidden {
		t.Fatalf("absent code %q", registry.Code(err))
	}

	silent := &stub{err: registry.ErrRoleSourceUnavailable}
	err = registry.Require(ctx, silent, "erin", contract.RoleReader)
	if !errors.Is(err, registry.ErrRoleSourceUnavailable) {
		t.Fatalf("silent: %v", err)
	}
	if registry.Code(err) != contract.CodeRoleSourceUnavailable {
		t.Fatalf("silent code %q", registry.Code(err))
	}
}

// TestForSource: the flag's two values and nothing else.
func TestForSource(t *testing.T) {
	for _, source := range []string{"", registry.SourceMeta} {
		r, err := registry.ForSource(source, nil)
		if err != nil || r != nil {
			t.Fatalf("%q: %v, %v — want the default", source, r, err)
		}
	}
	r, err := registry.ForSource(registry.SourceCallout, nil)
	if err != nil || r == nil {
		t.Fatalf("callout: %v, %v", r, err)
	}
	_, err = registry.ForSource("ldap", nil)
	if err == nil || !strings.Contains(err.Error(), "meta") || !strings.Contains(err.Error(), "callout") {
		t.Fatalf("ldap: %v", err)
	}
}
