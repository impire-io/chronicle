package mint_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/internal/mint"
	"github.com/impire-io/chronicle/internal/natstest"
)

// TestSealMakesControlTheAuthAccount is decision 0030 point 6 on a real
// server: after seal, CONTROL carries the external-authorization config —
// every account allowed, the callout xkey the bucket's auth entry holds
// the seed of, every user chronicle issued listed — and the sentinel is
// the one CONTROL user not listed, so it is gated: with no bridge
// answering, its connection is refused while a listed user's succeeds.
// No AUTH account exists, in the bucket or on disk.
func TestSealMakesControlTheAuthAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url, r := natstest.StartSealedOperator(t)
	sysConn, ctrlConn, custody := natstest.OpenInstance(t, url, r)

	ctrl, _, err := custody.Account(ctx, "CONTROL")
	if err != nil {
		t.Fatalf("CONTROL record: %v", err)
	}
	ac, err := jwt.DecodeAccountClaims(ctrl.JWT)
	if err != nil {
		t.Fatalf("decode CONTROL: %v", err)
	}
	if !ac.HasExternalAuthorization() {
		t.Fatal("CONTROL carries no callout config after seal")
	}
	if got := ac.Authorization.AllowedAccounts; len(got) != 1 || got[0] != jwt.AnyAccount {
		t.Fatalf("allowed accounts = %v, want [*]", got)
	}
	auth, _, err := custody.Auth(ctx)
	if err != nil {
		t.Fatalf("auth record: %v", err)
	}
	xkp, err := nkeys.FromCurveSeed([]byte(auth.XKeySeed))
	if err != nil {
		t.Fatalf("xkey seed in the bucket: %v", err)
	}
	if xpub, _ := xkp.PublicKey(); ac.Authorization.XKey != xpub {
		t.Fatalf("CONTROL's xkey %s is not the bucket's %s", ac.Authorization.XKey, xpub)
	}
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		t.Fatal(err)
	}
	instanceJWT, _ := jwt.ParseDecoratedJWT(bundle.ControlCreds)
	instance, _ := jwt.DecodeUserClaims(instanceJWT)
	if !ac.Authorization.AuthUsers.Contains(instance.Subject) || ctrl.Users["instance-1"] != instance.Subject {
		t.Fatalf("the first instance %s is not listed: auth_users %v, users %v", instance.Subject, ac.Authorization.AuthUsers, ctrl.Users)
	}
	sentinelJWT, _ := jwt.ParseDecoratedJWT([]byte(auth.SentinelCreds))
	sentinel, err := jwt.DecodeUserClaims(sentinelJWT)
	if err != nil {
		t.Fatalf("sentinel: %v", err)
	}
	if sentinel.IssuerAccount != "" && sentinel.IssuerAccount != ctrl.PublicKey {
		t.Fatalf("the sentinel belongs to %s, not CONTROL", sentinel.IssuerAccount)
	}
	if ac.Authorization.AuthUsers.Contains(sentinel.Subject) {
		t.Fatal("the sentinel is listed: it would bypass callout instead of triggering it")
	}
	if _, _, err := custody.Account(ctx, "AUTH"); !errors.Is(err, mint.ErrNoRecord) {
		t.Fatalf("an AUTH account survives in the bucket: %v", err)
	}
	for _, f := range []string{"auth-account.jwt", "auth-account.nk", "bridge.creds", "sentinel.creds"} {
		if _, err := os.Stat(filepath.Join(r.Dir, f)); !os.IsNotExist(err) {
			t.Fatalf("%s exists on the root", f)
		}
	}

	// The gate: no bridge answers, so the sentinel is refused; the listed
	// users and the tenants' users are untouched.
	if nc, err := mint.ConnectCreds(url, []byte(auth.SentinelCreds), "sentinel-ungated"); err == nil {
		nc.Close()
		t.Fatal("the sentinel connected with no bridge answering: it is not gated")
	}
	if !ctrlConn.IsConnected() || !sysConn.IsConnected() {
		t.Fatal("the instance's own connections did not survive the stamp")
	}
	d, err := mint.NewJWTDriver(ctx, custody, sysConn, url)
	if err != nil {
		t.Fatal(err)
	}
	// An instance added over the bucket is listed too — and connects.
	b, err := d.AddInstance(ctx, "cli-x", mint.TemplateCLI)
	if err != nil {
		t.Fatalf("add instance after the fold: %v", err)
	}
	nc, err := mint.ConnectCreds(url, b.ControlCreds, "cli-x")
	if err != nil {
		t.Fatalf("a listed user is gated: %v", err)
	}
	nc.Close()
	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint after the fold: %v", err)
	}
	svc, err := mint.IssueServiceUser(acct, "chronicle-node")
	if err != nil {
		t.Fatal(err)
	}
	tnc, err := mint.ConnectCreds(url, svc.File, "tenant-svc")
	if err != nil {
		t.Fatalf("a tenant user is gated by CONTROL's callout: %v", err)
	}
	tnc.Close()
}
