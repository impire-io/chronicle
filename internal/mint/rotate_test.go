package mint_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"

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

	r, err := mint.InitRoot(dir)
	if err != nil {
		t.Fatalf("init root: %v", err)
	}
	b := r.B
	srv, err := b.StartServer(-1)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	url := srv.ClientURL()
	if err := b.WriteClientURL(url); err != nil {
		t.Fatalf("write client url: %v", err)
	}
	bundle, err := mint.ReadBundle(r.BundleDir("instance-1"))
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}

	// A tenant minted under the old signing key, its material in the
	// sealed bucket the rotation's verification pass walks.
	sysConn, err := mint.ConnectCreds(url, bundle.SysCreds, "sys")
	if err != nil {
		t.Fatalf("connect sys: %v", err)
	}
	d := sealedDriver(ctx, t, r, bundle, url, sysConn)
	acct, err := d.MintAccount(ctx, "acme")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	member, err := mint.IssueMember(acct, "dana")
	if err != nil {
		t.Fatalf("issue member: %v", err)
	}
	if r2, err := mint.LoadRoot(dir); err != nil || r2.B.HasWorkingKeys() {
		t.Fatalf("the sealed root still holds working keys (%v)", err)
	}

	// The ceremony refuses a running fleet.
	if _, err := mint.RotateOperatorSigningKey(dir); err == nil || !strings.Contains(err.Error(), "running") {
		t.Fatalf("rotation against a running fleet must refuse, got %v", err)
	}

	sysConn.Close()
	srv.Shutdown()
	srv.WaitForShutdown()

	oldPub, err := mint.PublicKeyOfSeed(b.OperatorSigningSeed)
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

	// The rewritten operator JWT trusts only the new key, and the root
	// holds no working seed: the ceremony landed the new one in the bucket
	// and shredded it again.
	r2, err := mint.LoadRoot(dir)
	if err != nil {
		t.Fatalf("reload root: %v", err)
	}
	oc, err := jwt.DecodeOperatorClaims(r2.B.OperatorJWT)
	if err != nil {
		t.Fatalf("decode operator jwt: %v", err)
	}
	if !oc.SigningKeys.Contains(newPub) || oc.SigningKeys.Contains(oldPub) {
		t.Fatalf("operator signing keys not rotated: %v", oc.SigningKeys)
	}
	if r2.B.HasWorkingKeys() {
		t.Fatal("rotation left a working seed in the sealed root")
	}

	// The fleet comes back up trusting the new key: pre-rotation user
	// creds still connect, the bucket names the new key, and a fresh mint
	// under it succeeds.
	srv2, err := r2.B.StartServer(-1)
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

	sysConn2, err := mint.ConnectCreds(url2, bundle.SysCreds, "sys")
	if err != nil {
		t.Fatalf("connect sys after rotation: %v", err)
	}
	defer sysConn2.Close()
	ctrlConn2, err := mint.ConnectCreds(url2, bundle.ControlCreds, "control")
	if err != nil {
		t.Fatalf("connect control after rotation: %v", err)
	}
	defer ctrlConn2.Close()
	c2, err := mint.OpenCustody(ctx, ctrlConn2)
	if err != nil {
		t.Fatalf("open custody after rotation: %v", err)
	}
	op, _, err := c2.Operator(ctx)
	if err != nil || op.PublicKey != newPub {
		t.Fatalf("operator entry after rotation = %+v, %v; want %s", op, err, newPub)
	}
	d2, err := mint.NewJWTDriver(ctx, c2, sysConn2, url2)
	if err != nil {
		t.Fatalf("driver after rotation: %v", err)
	}
	if _, err := d2.MintAccount(ctx, "beta"); err != nil {
		t.Fatalf("mint under the rotated key: %v", err)
	}
}

// sealedDriver seals the root's material into the server's AUTH bucket
// and returns a driver over it — the shape every control instance boots
// into.
func sealedDriver(ctx context.Context, t *testing.T, r *mint.Root, bundle mint.Bundle, url string, sysConn *nats.Conn) *mint.JWTDriver {
	t.Helper()
	ctrlConn, err := mint.ConnectCreds(url, bundle.ControlCreds, "control")
	if err != nil {
		t.Fatalf("connect control: %v", err)
	}
	t.Cleanup(ctrlConn.Close)
	if _, err := r.Seal(ctx, sysConn, ctrlConn, mint.SealOptions{Replicas: 1}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	c, err := mint.OpenCustody(ctx, ctrlConn)
	if err != nil {
		t.Fatalf("open custody: %v", err)
	}
	d, err := mint.NewJWTDriver(ctx, c, sysConn, url)
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	return d
}
