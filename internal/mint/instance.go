package mint

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// Instances (chronicle-hq/02-DESIGN/10-custody.md § instances): a control
// instance holds one secret, its bundle — a CONTROL user under the
// control-instance template and a SYS user; a fleet instance (a workload
// service, an executor, the operator's CLI) holds a CONTROL user under the
// fleet template. Both are issued over the live bucket by any instance
// and recorded on the account they belong to, so removing one — losing a
// host — is a revocation found by name, not by the bundle the host took
// with it.

// instanceName holds the same line tenant names do: the name is a user
// name in server logs and a directory under the root.
var instanceName = regexp.MustCompile(`^[a-z0-9-]+$`)

func validInstanceName(name string) error {
	if !instanceName.MatchString(name) {
		return fmt.Errorf("instance name %q: lowercase letters, digits and '-' only", name)
	}
	return nil
}

// ErrInstanceExists says the name is taken: remove it first.
var ErrInstanceExists = errors.New("instance already exists")

// ErrNoSuchInstance says no account lists a user by that name.
var ErrNoSuchInstance = errors.New("no such instance")

// AddInstance issues an instance's users from the account seeds custody
// holds and records them on the accounts by compare-and-set. Nothing in
// the account JWTs changes before the AUTH fold, so nothing is pushed;
// the bundle comes back for the caller to write where the host reads it.
func (d *JWTDriver) AddInstance(ctx context.Context, name string, t Template) (Bundle, error) {
	if err := validInstanceName(name); err != nil {
		return Bundle{}, err
	}
	var bundle Bundle
	err := d.mutateAccount(ctx, "CONTROL", func(rec *AccountRecord, _ *jwt.AccountClaims) (bool, error) {
		if _, taken := rec.Users[name]; taken {
			return false, fmt.Errorf("%w: %s", ErrInstanceExists, name)
		}
		creds, err := issueAccountUser([]byte(rec.Seed), rec.PublicKey, name, t.Limits(name))
		if err != nil {
			return false, err
		}
		if rec.Users == nil {
			rec.Users = map[string]string{}
		}
		rec.Users[name] = creds.PublicKey
		bundle.ControlCreds = creds.File
		return false, nil
	})
	if err != nil {
		return Bundle{}, err
	}
	if t != TemplateControlInstance {
		return bundle, nil
	}
	err = d.mutateAccount(ctx, "SYS", func(rec *AccountRecord, _ *jwt.AccountClaims) (bool, error) {
		creds, err := issueAccountUser([]byte(rec.Seed), rec.PublicKey, name, jwt.UserPermissionLimits{})
		if err != nil {
			return false, err
		}
		if rec.Users == nil {
			rec.Users = map[string]string{}
		}
		rec.Users[name] = creds.PublicKey
		bundle.SysCreds = creds.File
		return false, nil
	})
	if err != nil {
		// Half an instance is no instance: take the CONTROL record back.
		_ = d.mutateAccount(ctx, "CONTROL", func(rec *AccountRecord, _ *jwt.AccountClaims) (bool, error) {
			delete(rec.Users, name)
			return false, nil
		})
		return Bundle{}, err
	}
	return bundle, nil
}

// RemoveInstance revokes the instance's users on every account that lists
// one — the re-signed JWT lands by compare-and-set, then pushes, so live
// connections are evicted and the bundle is dead wherever it went — and
// forgets the name.
func (d *JWTDriver) RemoveInstance(ctx context.Context, name string) error {
	if err := validInstanceName(name); err != nil {
		return err
	}
	found := false
	for _, account := range []string{"SYS", "CONTROL"} {
		err := d.mutateAccount(ctx, account, func(rec *AccountRecord, ac *jwt.AccountClaims) (bool, error) {
			pub, ok := rec.Users[name]
			if !ok {
				return false, nil
			}
			found = true
			ac.Revoke(pub)
			delete(rec.Users, name)
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	if !found {
		return fmt.Errorf("%w: %s", ErrNoSuchInstance, name)
	}
	return nil
}

// mutateAccount is a bootstrap account's mutation, the tenant shape held
// on the account records: read at a revision, let edit change the record
// and its claims, re-sign when the claims changed, land by compare-and-set
// at that revision, and push only then. A lost race re-reads and
// recomputes on top of the winner.
func (d *JWTDriver) mutateAccount(ctx context.Context, name string, edit func(*AccountRecord, *jwt.AccountClaims) (bool, error)) error {
	for attempt := 0; attempt < casAttempts; attempt++ {
		rec, rev, err := d.Custody.Account(ctx, name)
		if err != nil {
			return err
		}
		ac, err := jwt.DecodeAccountClaims(rec.JWT)
		if err != nil {
			return fmt.Errorf("decode account claims of %s: %w", name, err)
		}
		changed, err := edit(&rec, ac)
		if err != nil {
			return err
		}
		if changed {
			token, err := d.sign(ctx, ac)
			if err != nil {
				return err
			}
			rec.JWT = token
		}
		if _, err := d.Custody.PutAccount(ctx, rec, rev); err != nil {
			if errors.Is(err, ErrRevisionMismatch) {
				continue
			}
			return err
		}
		if changed {
			if err := d.push(ctx, rec.JWT); err != nil {
				return fmt.Errorf("push account %s: %w", name, err)
			}
		}
		return nil
	}
	return fmt.Errorf("account %s: %d writers raced this mutation and it never landed; retry", name, casAttempts)
}

// issueAccountUser issues a user of a bootstrap account from its seed —
// the root's before seal, the bucket's after.
func issueAccountUser(seed []byte, pub, name string, limits jwt.UserPermissionLimits) (Creds, error) {
	if len(seed) == 0 {
		return Creds{}, ErrNoAccountSeeds
	}
	akp, err := nkeys.FromSeed(seed)
	if err != nil {
		return Creds{}, fmt.Errorf("account seed: %w", err)
	}
	file, upub, err := issueDirectWith(akp, pub, name, limits)
	if err != nil {
		return Creds{}, err
	}
	return Creds{PublicKey: upub, File: file}, nil
}
