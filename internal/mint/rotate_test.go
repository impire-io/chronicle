package mint_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/internal/mint"
)

func TestBootstrapPersistsOperatorSeed(t *testing.T) {
	dir := t.TempDir()
	b, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("init bootstrap: %v", err)
	}
	if len(b.OperatorSeed) == 0 {
		t.Fatal("fresh bootstrap has no operator seed in memory")
	}
	if _, err := os.Stat(filepath.Join(dir, "operator.nk")); err != nil {
		t.Fatalf("operator.nk not persisted: %v", err)
	}
	reloaded, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("reload bootstrap: %v", err)
	}
	if string(reloaded.OperatorSeed) != string(b.OperatorSeed) {
		t.Fatal("reloaded operator seed differs from the minted one")
	}
}

func TestRotateRefusesPreCustodyInstall(t *testing.T) {
	dir := t.TempDir()
	if _, err := mint.LoadOrInitBootstrap(dir); err != nil {
		t.Fatalf("init bootstrap: %v", err)
	}
	// An install bootstrapped before operator-identity custody: no
	// operator.nk on disk. Loading stays fine; rotation refuses.
	if err := os.Remove(filepath.Join(dir, "operator.nk")); err != nil {
		t.Fatalf("remove operator.nk: %v", err)
	}
	b, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("pre-custody install must still load: %v", err)
	}
	if len(b.OperatorSeed) != 0 {
		t.Fatal("loaded a seed that is not on disk")
	}
	if _, err := mint.RotateOperatorSigningKey(dir); err == nil || !strings.Contains(err.Error(), "operator.nk") {
		t.Fatalf("rotation on a pre-custody install must refuse naming operator.nk, got %v", err)
	}
}

func TestRotateOperatorSigningKey(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	b, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("init bootstrap: %v", err)
	}
	srv, err := b.StartServer(-1)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	url := srv.ClientURL()
	if err := b.WriteClientURL(url); err != nil {
		t.Fatalf("write client url: %v", err)
	}

	// A tenant minted under the old signing key, with the on-disk custody
	// the rotation's verification pass walks.
	sysConn, err := mint.ConnectCreds(url, b.SysCreds, "sys")
	if err != nil {
		t.Fatalf("connect sys: %v", err)
	}
	d := &mint.JWTDriver{OperatorSigningSeed: b.OperatorSigningSeed, SysConn: sysConn, URL: url}
	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	svc, err := mint.IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		t.Fatalf("issue service: %v", err)
	}
	member, err := mint.IssueMember(acct, "dana")
	if err != nil {
		t.Fatalf("issue member: %v", err)
	}
	tenantDir := filepath.Join(b.AccountsDir(), "acme")
	if err := os.MkdirAll(tenantDir, 0o700); err != nil {
		t.Fatalf("tenant dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tenantDir, "service.creds"), svc.File, 0o600); err != nil {
		t.Fatalf("write service creds: %v", err)
	}

	// The ceremony refuses a running fleet.
	if _, err := mint.RotateOperatorSigningKey(dir); err == nil || !strings.Contains(err.Error(), "running") {
		t.Fatalf("rotation against a running fleet must refuse, got %v", err)
	}

	sysConn.Close()
	srv.Shutdown()
	srv.WaitForShutdown()

	oldSigning, err := nkeys.FromSeed(b.OperatorSigningSeed)
	if err != nil {
		t.Fatalf("old signing seed: %v", err)
	}
	oldPub, err := oldSigning.PublicKey()
	if err != nil {
		t.Fatalf("old signing pub: %v", err)
	}
	newPub, err := mint.RotateOperatorSigningKey(dir)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if newPub == oldPub {
		t.Fatal("rotation returned the old signing key")
	}

	// The rewritten operator JWT trusts only the new key.
	b2, err := mint.LoadOrInitBootstrap(dir)
	if err != nil {
		t.Fatalf("reload bootstrap: %v", err)
	}
	oc, err := jwt.DecodeOperatorClaims(b2.OperatorJWT)
	if err != nil {
		t.Fatalf("decode operator jwt: %v", err)
	}
	if !oc.SigningKeys.Contains(newPub) || oc.SigningKeys.Contains(oldPub) {
		t.Fatalf("operator signing keys not rotated: %v", oc.SigningKeys)
	}

	// The fleet comes back up trusting the new key: pre-rotation user
	// creds still connect, and a fresh mint under the new key succeeds.
	srv2, err := b2.StartServer(-1)
	if err != nil {
		t.Fatalf("restart server: %v", err)
	}
	defer func() {
		srv2.Shutdown()
		srv2.WaitForShutdown()
	}()
	url2 := srv2.ClientURL()

	mnc, err := mint.ConnectCreds(url2, member.File, "dana")
	if err != nil {
		t.Fatalf("pre-rotation member creds refused after rotation: %v", err)
	}
	mnc.Close()

	sysConn2, err := mint.ConnectCreds(url2, b2.SysCreds, "sys")
	if err != nil {
		t.Fatalf("connect sys after rotation: %v", err)
	}
	defer sysConn2.Close()
	d2 := &mint.JWTDriver{OperatorSigningSeed: b2.OperatorSigningSeed, SysConn: sysConn2, URL: url2}
	if _, err := d2.MintAccount(ctx, "beta"); err != nil {
		t.Fatalf("mint under the rotated key: %v", err)
	}
}
