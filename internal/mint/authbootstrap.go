package mint

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// The AUTH account of decision 0026 (02-DESIGN/08-browser-identity.md):
// the third bootstrap account, whose only job is triggering the callout
// bridge. Its external-authorization config names control's bridge user,
// allows placement into any account (the source-verified "*" wildcard —
// the mint never re-pushes AUTH), and declares an xkey so requests travel
// encrypted. Existing bootstrap dirs gain it on load, the
// ensureControlJetStream way.

const (
	fAuthAcctJWT   = "auth-account.jwt"
	fAuthAcctPub   = "auth-account.pub"
	fAuthAcctNK    = "auth-account.nk"
	fAuthXKeyNK    = "auth-xkey.nk"
	fBridgeCreds   = "bridge.creds"
	fSentinelCreds = "sentinel.creds"
)

// ensureAuthAccount loads the AUTH material, generating and persisting it
// when absent. Called from both the init and load paths; needs only the
// operator signing seed, which every bootstrap holds.
func (b *Bootstrap) ensureAuthAccount() error {
	if _, err := os.Stat(filepath.Join(b.Dir, fAuthAcctJWT)); err == nil {
		return b.loadAuthAccount()
	}
	oskp, err := nkeys.FromSeed(b.OperatorSigningSeed)
	if err != nil {
		return fmt.Errorf("operator signing seed: %w", err)
	}
	auth, err := newAuthMaterial(oskp)
	if err != nil {
		return err
	}
	files := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{fAuthAcctJWT, []byte(auth.jwt), plainFileMode},
		{fAuthAcctPub, []byte(auth.pub), plainFileMode},
		{fAuthAcctNK, auth.seed, keyFileMode},
		{fAuthXKeyNK, auth.xkeySeed, keyFileMode},
		{fBridgeCreds, auth.bridgeCreds, keyFileMode},
		{fSentinelCreds, auth.sentinelCreds, plainFileMode},
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(b.Dir, f.name), f.data, f.mode); err != nil {
			return fmt.Errorf("write %s: %w", f.name, err)
		}
	}
	auth.applyTo(b)
	return nil
}

// authMaterial is the AUTH account born in one piece: its key, its xkey,
// its two users, and its JWT under the operator signing key.
type authMaterial struct {
	pub, jwt      string
	seed          []byte
	xkeySeed      []byte
	bridgeCreds   []byte
	sentinelCreds []byte
}

func (a authMaterial) applyTo(b *Bootstrap) {
	b.AuthAccountPub = a.pub
	b.AuthAccountJWT = a.jwt
	b.AuthAccountSeed = a.seed
	b.AuthXKeySeed = a.xkeySeed
	b.BridgeCreds = a.bridgeCreds
	b.SentinelCreds = a.sentinelCreds
}

// newAuthMaterial births the AUTH account under the operator signing key —
// the dev root writes it to disk, the material seal pushes it straight
// to the resolver and the bucket.
func newAuthMaterial(oskp nkeys.KeyPair) (authMaterial, error) {
	akp, apub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return authMaterial{}, fmt.Errorf("auth account key: %w", err)
	}
	xkp, err := nkeys.CreateCurveKeys()
	if err != nil {
		return authMaterial{}, fmt.Errorf("auth xkey: %w", err)
	}
	xpub, err := xkp.PublicKey()
	if err != nil {
		return authMaterial{}, fmt.Errorf("auth xkey public: %w", err)
	}
	xseed, err := xkp.Seed()
	if err != nil {
		return authMaterial{}, fmt.Errorf("auth xkey seed: %w", err)
	}
	aseed, err := akp.Seed()
	if err != nil {
		return authMaterial{}, fmt.Errorf("auth account seed: %w", err)
	}

	// The bridge user bypasses callout (auth_users) — control connects
	// with it to answer requests. The sentinel triggers callout — it is
	// public by design, worthless without a valid identity behind it.
	bridgeCreds, bridgePub, err := issueDirect(akp, apub, "chronicle-bridge")
	if err != nil {
		return authMaterial{}, fmt.Errorf("bridge user: %w", err)
	}
	sentinelCreds, _, err := issueDirect(akp, apub, "sentinel")
	if err != nil {
		return authMaterial{}, fmt.Errorf("sentinel user: %w", err)
	}

	ac := jwt.NewAccountClaims(apub)
	ac.Name = "AUTH"
	ac.Authorization = jwt.ExternalAuthorization{
		AuthUsers:       jwt.StringList{bridgePub},
		AllowedAccounts: jwt.StringList{jwt.AnyAccount},
		XKey:            xpub,
	}
	authJWT, err := ac.Encode(oskp)
	if err != nil {
		return authMaterial{}, fmt.Errorf("encode auth account jwt: %w", err)
	}
	return authMaterial{pub: apub, jwt: authJWT, seed: aseed, xkeySeed: xseed, bridgeCreds: bridgeCreds, sentinelCreds: sentinelCreds}, nil
}

// loadAuthAccount reads the AUTH material: the JWT and public key every
// root keeps, and the seeds and users seal shreds — absent after it, held
// by the bucket.
func (b *Bootstrap) loadAuthAccount() error {
	var firstErr error
	read := func(name string) []byte {
		p, err := os.ReadFile(filepath.Join(b.Dir, name))
		if err != nil && firstErr == nil {
			firstErr = err
		}
		return p
	}
	optional := func(name string) []byte {
		p, err := os.ReadFile(filepath.Join(b.Dir, name))
		if err != nil {
			return nil
		}
		return p
	}
	authJWT := read(fAuthAcctJWT)
	authPub := read(fAuthAcctPub)
	if firstErr != nil {
		return fmt.Errorf("auth account material incomplete in %s: %w", b.Dir, firstErr)
	}
	b.AuthAccountJWT = string(authJWT)
	b.AuthAccountPub = string(authPub)
	b.AuthAccountSeed = optional(fAuthAcctNK)
	b.AuthXKeySeed = optional(fAuthXKeyNK)
	b.BridgeCreds = optional(fBridgeCreds)
	b.SentinelCreds = optional(fSentinelCreds)
	return nil
}
