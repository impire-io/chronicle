package mint

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// The callout on CONTROL (chronicle-hq/02-DESIGN/10-custody.md § the AUTH
// account folds into CONTROL, decision 0030 point 6): CONTROL is the auth
// account. Its JWT carries the external-authorization config — every
// account allowed, the xkey requests are sealed to — and lists every user
// chronicle issued, which bypass callout; the sentinel is the one CONTROL
// user that is not listed, and is therefore always gated. The bridge
// answers on a control instance's own connection and signs with CONTROL's
// account key. Chronicle's platform accounts are SYS, which is NATS's,
// and CONTROL.

// fAuthXKeyNK is the dev root's curve seed before seal; the bucket's
// `auth` entry holds it after.
const fAuthXKeyNK = "auth-xkey.nk"

// ensureXKey births the dev root's callout xkey at init.
func (b *Bootstrap) ensureXKey() error {
	if _, err := os.Stat(filepath.Join(b.Dir, fAuthXKeyNK)); err == nil {
		return b.loadXKey()
	}
	xkp, err := nkeys.CreateCurveKeys()
	if err != nil {
		return fmt.Errorf("callout xkey: %w", err)
	}
	seed, err := xkp.Seed()
	if err != nil {
		return fmt.Errorf("callout xkey seed: %w", err)
	}
	if err := os.WriteFile(filepath.Join(b.Dir, fAuthXKeyNK), seed, keyFileMode); err != nil {
		return fmt.Errorf("write %s: %w", fAuthXKeyNK, err)
	}
	b.AuthXKeySeed = seed
	return nil
}

// loadXKey reads the dev root's xkey — present until seal shreds it.
func (b *Bootstrap) loadXKey() error {
	seed, err := os.ReadFile(filepath.Join(b.Dir, fAuthXKeyNK))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read %s: %w", fAuthXKeyNK, err)
	}
	b.AuthXKeySeed = seed
	return nil
}

// newXKey is a fresh callout xkey — the material seal's, born at seal.
func newXKey() ([]byte, error) {
	xkp, err := nkeys.CreateCurveKeys()
	if err != nil {
		return nil, fmt.Errorf("callout xkey: %w", err)
	}
	return xkp.Seed()
}

// stampCallout puts the external-authorization config on CONTROL's JWT:
// the xkey, every account allowed, and the given users listed as
// bypassing callout, joining any already listed. A JWT that carries all
// of it comes back unchanged.
func stampCallout(token string, oskp nkeys.KeyPair, xkeySeed []byte, authUsers []string) (string, bool, error) {
	xkp, err := nkeys.FromCurveSeed(xkeySeed)
	if err != nil {
		return "", false, fmt.Errorf("callout xkey seed: %w", err)
	}
	xpub, err := xkp.PublicKey()
	if err != nil {
		return "", false, err
	}
	ac, err := jwt.DecodeAccountClaims(token)
	if err != nil {
		return "", false, fmt.Errorf("decode control account jwt: %w", err)
	}
	changed := false
	if ac.Authorization.XKey != xpub {
		ac.Authorization.XKey = xpub
		changed = true
	}
	if !ac.Authorization.AllowedAccounts.Contains(jwt.AnyAccount) {
		ac.Authorization.AllowedAccounts = jwt.StringList{jwt.AnyAccount}
		changed = true
	}
	for _, u := range authUsers {
		if !ac.Authorization.AuthUsers.Contains(u) {
			ac.Authorization.AuthUsers.Add(u)
			changed = true
		}
	}
	if !changed {
		return token, false, nil
	}
	stamped, err := ac.Encode(oskp)
	if err != nil {
		return "", false, fmt.Errorf("re-encode control account jwt: %w", err)
	}
	return stamped, true, nil
}

// issueSentinel mints the sentinel: a CONTROL user that is never listed
// in auth_users, so every connection with it is routed through callout.
// Public by design (0026) — worthless without a valid identity behind it.
func issueSentinel(controlSeed []byte, controlPub string) ([]byte, error) {
	creds, err := issueAccountUser(controlSeed, controlPub, "sentinel", jwt.UserPermissionLimits{})
	if err != nil {
		return nil, fmt.Errorf("sentinel: %w", err)
	}
	return creds.File, nil
}
