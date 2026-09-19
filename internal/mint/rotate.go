package mint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/impire-io/chronicle/internal/devdir"
)

// RotateOperatorSigningKey re-keys an offline install's trust root: a new
// operator signing key replaces the old one in the operator JWT, and every
// account JWT — SYS, CONTROL, every tenant — is re-signed and rewritten in
// place. Trusted operator keys are fixed at server boot, so the ceremony
// only runs against a stopped fleet; the next start trusts only the new
// key. User credentials survive untouched: they are signed by account-level
// keys, and the re-signed account JWTs keep their account identities.
//
// The ceremony is single-substrate: it rewrites this directory's resolver
// files directly. Rotating a multi-server resolver is the hosted
// environment's open question, not this function's.
//
// It returns the new signing key's public key, and verifies the result the
// only way that counts: boot the server, connect with every credential the
// install holds, shut down.
func RotateOperatorSigningKey(dir string) (string, error) {
	if _, err := os.Stat(filepath.Join(dir, fOperatorJWT)); err != nil {
		return "", fmt.Errorf("no bootstrap at %s: %w", dir, err)
	}
	r, err := LoadRoot(dir)
	if err != nil {
		return "", err
	}
	b := r.B
	if len(b.OperatorSeed) == 0 {
		return "", fmt.Errorf("%s has no %s: the install predates operator-identity custody, and its operator JWT can never be re-issued — rotation needs a fresh bootstrap", dir, fOperatorNK)
	}
	sysCreds, _, err := ceremonyCreds(r)
	if err != nil {
		return "", err
	}
	if err := requireStopped(dir, sysCreds); err != nil {
		return "", err
	}

	okp, err := nkeys.FromSeed(b.OperatorSeed)
	if err != nil {
		return "", fmt.Errorf("operator seed: %w", err)
	}
	opub, err := okp.PublicKey()
	if err != nil {
		return "", fmt.Errorf("operator public key: %w", err)
	}
	oc, err := jwt.DecodeOperatorClaims(b.OperatorJWT)
	if err != nil {
		return "", fmt.Errorf("decode operator jwt: %w", err)
	}
	if oc.Subject != opub {
		return "", fmt.Errorf("%s does not match the operator JWT: seed is for %s, operator is %s", fOperatorNK, opub, oc.Subject)
	}

	oskp2, ospub2, err := newKey(nkeys.CreateOperator)
	if err != nil {
		return "", fmt.Errorf("new operator signing key: %w", err)
	}
	oc.SigningKeys = jwt.StringList{ospub2}
	operatorJWT2, err := oc.Encode(okp)
	if err != nil {
		return "", fmt.Errorf("re-encode operator jwt: %w", err)
	}

	// Each account JWT is encoded exactly once and that string lands
	// everywhere it rests: the resolver keeps the newest-issued copy, so a
	// second encode with a later timestamp would shadow the first.
	sysJWT2, err := resignAccount(b.SystemAccountJWT, oskp2)
	if err != nil {
		return "", fmt.Errorf("re-sign system account: %w", err)
	}
	ctrlJWT2, err := resignAccount(b.ControlAccountJWT, oskp2)
	if err != nil {
		return "", fmt.Errorf("re-sign control account: %w", err)
	}
	authJWT2, err := resignAccount(b.AuthAccountJWT, oskp2)
	if err != nil {
		return "", fmt.Errorf("re-sign auth account: %w", err)
	}
	resigned := map[string]string{
		b.SystemAccountPub:  sysJWT2,
		b.ControlAccountPub: ctrlJWT2,
		b.AuthAccountPub:    authJWT2,
	}

	// The resolver dir is flat <ACCOUNTPUB>.jwt files, indexed from
	// content at boot — rewriting them against a stopped server is the
	// whole point of the offline ceremony.
	resolver := filepath.Join(dir, resolverDir)
	entries, err := os.ReadDir(resolver)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read resolver dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jwt") {
			continue
		}
		path := filepath.Join(resolver, e.Name())
		token, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", e.Name(), err)
		}
		ac, err := jwt.DecodeAccountClaims(string(token))
		if err != nil {
			return "", fmt.Errorf("decode %s: %w", e.Name(), err)
		}
		fresh, ok := resigned[ac.Subject]
		if !ok {
			if fresh, err = resignAccount(string(token), oskp2); err != nil {
				return "", fmt.Errorf("re-sign %s: %w", ac.Name, err)
			}
		}
		if err := os.WriteFile(path, []byte(fresh), plainFileMode); err != nil {
			return "", fmt.Errorf("write %s: %w", e.Name(), err)
		}
	}

	osSeed2, err := oskp2.Seed()
	if err != nil {
		return "", fmt.Errorf("new signing seed: %w", err)
	}
	files := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{fOperatorJWT, []byte(operatorJWT2), plainFileMode},
		{fSysAcctJWT, []byte(sysJWT2), plainFileMode},
		{fCtrlAcctJWT, []byte(ctrlJWT2), plainFileMode},
		{fAuthAcctJWT, []byte(authJWT2), plainFileMode},
		// The signing seed goes last: every earlier write is consistent
		// with either seed on disk, so a crash mid-ceremony is re-runnable.
		{fOperatorSK, osSeed2, keyFileMode},
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, f.mode); err != nil {
			return "", fmt.Errorf("write %s: %w", f.name, err)
		}
	}

	if err := verifyRotated(dir); err != nil {
		return "", fmt.Errorf("verify rotated install: %w", err)
	}
	return ospub2, nil
}

// ceremonyCreds is what the directory ceremony dials with: the root's
// control-instance bundle once one exists — after seal it is all the root
// holds — and the bootstrap users before any bundle was issued.
func ceremonyCreds(r *Root) (sys, ctrl []byte, err error) {
	if _, bundle, err := r.ControlBundle(""); err == nil {
		return bundle.SysCreds, bundle.ControlCreds, nil
	}
	if len(r.B.SysCreds) > 0 && len(r.B.ControlCreds) > 0 {
		return r.B.SysCreds, r.B.ControlCreds, nil
	}
	return nil, nil, fmt.Errorf("%s holds no credentials to run the ceremony with: no control-instance bundle and no bootstrap users", r.Dir)
}

// requireStopped refuses the ceremony while the fleet answers: the running
// server trusts the old key and would fight the rewrite. A stale
// client.url after a clean stop is normal — the dial is the test. The dial
// carries the install's own sys creds; an authorization refusal still
// proves something is listening.
func requireStopped(dir string, sysCreds []byte) error {
	url, err := devdir.ReadClientURL(dir)
	if err != nil {
		return nil
	}
	token, jwtErr := jwt.ParseDecoratedJWT(sysCreds)
	kp, kpErr := jwt.ParseDecoratedUserNKey(sysCreds)
	if jwtErr != nil || kpErr != nil {
		return fmt.Errorf("parse sys creds: %w", errors.Join(jwtErr, kpErr))
	}
	nc, err := nats.Connect(url,
		nats.Name("chronicle-rotate-guard"),
		nats.UserJWT(
			func() (string, error) { return token, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.Timeout(2*time.Second),
		nats.RetryOnFailedConnect(false),
		nats.MaxReconnects(0),
	)
	if err == nil {
		nc.Close()
	} else if !errors.Is(err, nats.ErrAuthorization) {
		return nil
	}
	return fmt.Errorf("the fleet at %s is still running (%s answers); stop it before rotating", dir, url)
}

func resignAccount(token string, signer nkeys.KeyPair) (string, error) {
	ac, err := jwt.DecodeAccountClaims(token)
	if err != nil {
		return "", err
	}
	return ac.Encode(signer)
}

// verifyRotated boots the rewritten install, brings custody in step with
// the rewritten directory when the root is sealed — and shreds the new
// seed from the root again once the bucket holds it — and connects with
// every credential the install holds: the ceremony's own, and each
// tenant's service user. The could-not-succeed-if-broken read: a bad
// rewrite cannot pass it.
func verifyRotated(dir string) error {
	r, err := LoadRoot(dir)
	if err != nil {
		return err
	}
	b := r.B
	key := ""
	if n, ok := r.Manifest.Nodes["embedded"]; ok {
		key = n.JetStreamKey
	}
	sysCreds, ctrlCreds, err := ceremonyCreds(r)
	if err != nil {
		return err
	}
	srv, err := b.StartServerWithKey(-1, key)
	if err != nil {
		return err
	}
	// A full shutdown wait: the caller may boot this directory again
	// immediately, and the JetStream store must be released first.
	defer func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	}()
	url := srv.ClientURL()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	creds := map[string][]byte{"sys": sysCreds, "control": ctrlCreds}
	ctrlConn, err := ConnectCreds(url, ctrlCreds, "rotate-verify-custody")
	if err != nil {
		return fmt.Errorf("control cannot connect after rotation: %w", err)
	}
	defer ctrlConn.Close()
	c, err := OpenCustody(ctx, ctrlConn)
	switch {
	case errors.Is(err, ErrNotSealed):
		// An unsealed directory: nothing but the files to keep in step.
	case err != nil:
		return err
	default:
		if err := rotateCustody(ctx, c, dir, b); err != nil {
			return fmt.Errorf("bring custody in step: %w", err)
		}
		// A sealed root keeps no working seed: the bucket has the new one.
		if err := shredWorkingKeys(dir); err != nil {
			return err
		}
		names, err := c.Tenants(ctx)
		if err != nil {
			return err
		}
		for _, name := range names {
			rec, _, err := c.Tenant(ctx, name)
			if err != nil {
				return err
			}
			creds["tenant "+name] = []byte(rec.ServiceCreds)
		}
	}
	for name, cr := range creds {
		nc, err := ConnectCreds(url, cr, "rotate-verify")
		if err != nil {
			return fmt.Errorf("%s cannot connect after rotation: %w", name, err)
		}
		nc.Close()
	}
	return nil
}

// rotateCustody lands the rewritten directory's material in the bucket:
// the new signing key in the operator entry, and every account's freshly
// signed JWT — read back from the resolver dir the ceremony rewrote — in
// its record, each by compare-and-set at the revision it was read at. The
// bucket stays canonical across the directory ceremony; the live, rolling
// ceremony that replaces this one is design 10 § rotation.
func rotateCustody(ctx context.Context, c *Custody, dir string, b *Bootstrap) error {
	opPub, err := PublicKeyOfSeed(b.OperatorSigningSeed)
	if err != nil {
		return err
	}
	op, rev, err := c.Operator(ctx)
	if err != nil {
		return err
	}
	if op.SigningSeed != string(b.OperatorSigningSeed) {
		op.PublicKey, op.SigningSeed = opPub, string(b.OperatorSigningSeed)
		if _, err := c.PutOperator(ctx, op, rev); err != nil {
			return fmt.Errorf("operator: %w", err)
		}
	}
	fresh := func(pub string) (string, error) {
		token, err := os.ReadFile(filepath.Join(dir, resolverDir, pub+".jwt"))
		if err != nil {
			return "", fmt.Errorf("re-signed jwt of %s: %w", pub, err)
		}
		return string(token), nil
	}
	for _, name := range []string{"SYS", "CONTROL", "AUTH"} {
		rec, rev, err := c.Account(ctx, name)
		if errors.Is(err, ErrNoRecord) {
			continue
		}
		if err != nil {
			return err
		}
		token, err := fresh(rec.PublicKey)
		if err != nil {
			return err
		}
		if token == rec.JWT {
			continue
		}
		rec.JWT = token
		if _, err := c.PutAccount(ctx, rec, rev); err != nil {
			return fmt.Errorf("account %s: %w", name, err)
		}
	}
	names, err := c.Tenants(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		rec, rev, err := c.Tenant(ctx, name)
		if err != nil {
			return err
		}
		token, err := fresh(rec.PublicKey)
		if err != nil {
			return err
		}
		if token == rec.JWT {
			continue
		}
		rec.JWT = token
		if _, err := c.PutTenant(ctx, rec, rev); err != nil {
			return fmt.Errorf("tenant %s: %w", name, err)
		}
	}
	return nil
}
