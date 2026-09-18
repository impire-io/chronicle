package control_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/internal/control"
	"github.com/impire-io/chronicle/internal/identity/github"
	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/natstest"
)

// fakeValidator is the GitHub half of the bridge as a test double — the
// NATS half runs against the real embedded operator server, per the
// no-mocked-NATS rule.
type fakeValidator struct {
	mu     sync.Mutex
	tokens map[string]github.Identity
}

func (f *fakeValidator) ValidateToken(_ context.Context, token string) (github.Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.tokens[token]
	if !ok {
		return github.Identity{}, github.ErrInvalidToken
	}
	return id, nil
}

// TestBridgePlacesGithubIdentities is spec 017's could-not-succeed-if-
// broken read: a real operator-mode server with the AUTH account's
// external authorization live, control's bridge answering the callout,
// a tenant minted and a membership bound to a GitHub ID — then a sentinel
// + token connect lands in the tenant as that principal, publishes where
// the tenant's own service user can hear it, and every refusal path
// refuses.
func TestBridgePlacesGithubIdentities(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	url, b := natstest.StartOperator(t)

	sysConn, err := mint.ConnectCreds(url, b.SysCreds, "test-sys")
	if err != nil {
		t.Fatalf("connect sys: %v", err)
	}
	t.Cleanup(sysConn.Close)
	ctrlConn, err := mint.ConnectCreds(url, b.ControlCreds, "test-control")
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	t.Cleanup(ctrlConn.Close)
	authConn, err := mint.ConnectCreds(url, b.BridgeCreds, "test-bridge")
	if err != nil {
		t.Fatalf("connect bridge user: %v", err)
	}
	t.Cleanup(authConn.Close)

	validator := &fakeValidator{tokens: map[string]github.Identity{
		"gh-erin": {ID: 12345, Login: "erin"},
	}}
	accountsDir := t.TempDir()
	svc, err := control.Start(ctrlConn, control.Config{
		Driver: &mint.JWTDriver{
			OperatorSigningSeed: b.OperatorSigningSeed,
			SysConn:             sysConn,
			URL:                 url,
			ControlAccountPub:   b.ControlAccountPub,
		},
		URL:         url,
		AccountsDir: accountsDir,
		Bridge: &control.BridgeConfig{
			Conn:               authConn,
			ResponseSignerSeed: b.AuthAccountSeed,
			XKeySeed:           b.AuthXKeySeed,
			Validator:          validator,
		},
	})
	if err != nil {
		t.Fatalf("start control: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })

	// A tenant with one GitHub-bound member. The mint and the binding go
	// through the real verbs.
	ctl := client.NewControl(ctrlConn)
	if _, err := ctl.MintTenant(ctx, "acme", "boss"); err != nil {
		t.Fatalf("mint tenant: %v", err)
	}
	if _, err := ctl.AddMember(ctx, "acme", "erin", "writer", 12345); err != nil {
		t.Fatalf("add member: %v", err)
	}

	// The tenant's own service user listens where the member baseline may
	// publish — placement lands in the same account or nowhere. The creds
	// come off the accounts dir control persists them to; the test stands
	// in for the executor's record-verified pull, whose fleet log this
	// test deliberately does not run.
	tenantSvcCreds, err := os.ReadFile(filepath.Join(accountsDir, "acme", "service.creds"))
	if err != nil {
		t.Fatalf("read tenant service creds: %v", err)
	}
	svcConn, err := mint.ConnectCreds(url, tenantSvcCreds, "test-tenant-svc")
	if err != nil {
		t.Fatalf("connect tenant service: %v", err)
	}
	t.Cleanup(svcConn.Close)
	heard := make(chan string, 1)
	if _, err := svcConn.Subscribe("CHRON.probe", func(m *nats.Msg) { heard <- string(m.Data) }); err != nil {
		t.Fatalf("subscribe probe: %v", err)
	}
	if err := svcConn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The bridge dial: sentinel + tenant:token, placed as erin.
	c, err := client.ConnectBridge(url, b.SentinelCreds, "acme", "gh-erin")
	if err != nil {
		t.Fatalf("bridge connect: %v", err)
	}
	t.Cleanup(c.Close)
	if c.Author() != "erin" {
		t.Fatalf("placed author = %q, want erin", c.Author())
	}
	if err := c.Conn().Publish("CHRON.probe", []byte("hello from the bridge")); err != nil {
		t.Fatalf("publish in tenant: %v", err)
	}
	select {
	case got := <-heard:
		if got != "hello from the bridge" {
			t.Fatalf("heard %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the tenant's service user never heard the bridged member: wrong account?")
	}

	// Refusals — each the same uniform authentication failure on the wire.
	refusals := []struct {
		name, tenant, token string
	}{
		{"unknown token", "acme", "gh-nobody"},
		{"unknown tenant", "ghost", "gh-erin"},
		{"malformed token", "acme", ""},
	}
	for _, tc := range refusals {
		if _, err := client.ConnectBridge(url, b.SentinelCreds, tc.tenant, tc.token); err == nil {
			t.Errorf("%s: connect must refuse", tc.name)
		} else if !strings.Contains(strings.ToLower(err.Error()), "auth") && tc.token != "" {
			t.Errorf("%s: want an authentication error, got %v", tc.name, err)
		}
	}

	// An unbound GitHub identity refuses even with a valid token.
	validator.mu.Lock()
	validator.tokens["gh-frank"] = github.Identity{ID: 999, Login: "frank"}
	validator.mu.Unlock()
	if _, err := client.ConnectBridge(url, b.SentinelCreds, "acme", "gh-frank"); err == nil {
		t.Error("unbound identity must refuse")
	}
}
