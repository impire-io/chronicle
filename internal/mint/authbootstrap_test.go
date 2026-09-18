package mint_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/jwt/v2"

	"github.com/impire-io/chronicle/internal/mint"
)

// The AUTH account is bootstrap material like SYS and CONTROL: born with a
// fresh install, grown into an existing one on load — the
// ensureControlJetStream way.
func TestBootstrapCarriesAuthAccount(t *testing.T) {
	dir := t.TempDir()
	b, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	assertAuth := func(b *mint.Bootstrap) {
		t.Helper()
		ac, err := jwt.DecodeAccountClaims(b.AuthAccountJWT)
		if err != nil {
			t.Fatalf("decode auth jwt: %v", err)
		}
		if !ac.HasExternalAuthorization() {
			t.Fatal("AUTH account carries no external authorization")
		}
		if got := ac.Authorization.AllowedAccounts; len(got) != 1 || got[0] != jwt.AnyAccount {
			t.Fatalf("allowed_accounts = %v, want the wildcard", got)
		}
		if ac.Authorization.XKey == "" {
			t.Fatal("no xkey on the AUTH account")
		}
		if len(b.SentinelCreds) == 0 || len(b.BridgeCreds) == 0 || len(b.AuthAccountSeed) == 0 || len(b.AuthXKeySeed) == 0 {
			t.Fatal("auth material incomplete on the bootstrap")
		}
		// The bridge user is the auth_users bypass; the sentinel is not.
		bridgeJWT, err := jwt.ParseDecoratedJWT(b.BridgeCreds)
		if err != nil {
			t.Fatalf("parse bridge creds: %v", err)
		}
		buc, err := jwt.DecodeUserClaims(bridgeJWT)
		if err != nil {
			t.Fatalf("decode bridge user: %v", err)
		}
		if !ac.Authorization.AuthUsers.Contains(buc.Subject) {
			t.Fatal("bridge user is not in auth_users")
		}
		sentinelJWT, err := jwt.ParseDecoratedJWT(b.SentinelCreds)
		if err != nil {
			t.Fatalf("parse sentinel creds: %v", err)
		}
		suc, err := jwt.DecodeUserClaims(sentinelJWT)
		if err != nil {
			t.Fatalf("decode sentinel user: %v", err)
		}
		if ac.Authorization.AuthUsers.Contains(suc.Subject) {
			t.Fatal("the sentinel must trigger callout, not bypass it")
		}
	}
	assertAuth(b)

	// An install bootstrapped before the bridge existed grows the AUTH
	// account on load.
	for _, f := range []string{"auth-account.jwt", "auth-account.pub", "auth-account.nk", "auth-xkey.nk", "bridge.creds", "sentinel.creds"} {
		if err := os.Remove(filepath.Join(dir, f)); err != nil {
			t.Fatalf("remove %s: %v", f, err)
		}
	}
	upgraded, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	assertAuth(upgraded)

	// And a plain reload keeps what it has.
	again, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("second reload: %v", err)
	}
	if again.AuthAccountPub != upgraded.AuthAccountPub {
		t.Fatal("reload regenerated the AUTH account instead of loading it")
	}
}
