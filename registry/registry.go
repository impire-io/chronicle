// Package registry reads the identity slice of META — the membership
// registry of decision 0007 — for the role checks that gate the
// CHRON.API.> surface (04-fleet.md), and seeds it where no minter does.
// Every API-serving component runs the same check; a component carrying
// its own copy is a drift hazard.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// SeedOpt adjusts the admin membership Seed records.
type SeedOpt func(*contract.Membership)

// WithGithubID binds the seeded admin to a GitHub identity — the numeric
// user id — for the browser bridge: a self-service account's admin logs
// in through the bridge and may hold no issued credential at all
// (decision 0035). Zero binds nothing.
func WithGithubID(id int64) SeedOpt {
	return func(m *contract.Membership) { m.GithubID = id }
}

// Seed births an account's registry where no service minted it — the
// open form (11-the-two-forms.md § membership, without custody): META
// create-if-absent, and the one admin principal and membership, each
// create-if-absent, so a second run against the same account changes
// nothing. The public key is the admin's NATS user where a minter issued
// one; in the open form it is empty — the identity is the operator's NATS
// user, and the principal is asserted. The managed service seeds the same
// way at every mint, binding the admin to its GitHub identity where the
// account is the identity's own.
func Seed(ctx context.Context, js jetstream.JetStream, admin, publicKey string, opts ...SeedOpt) (jetstream.KeyValue, error) {
	if err := contract.ValidatePrincipalName(admin); err != nil {
		return nil, fmt.Errorf("seed: %w", err)
	}
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		meta, err = js.CreateKeyValue(ctx, contract.MetaBucketConfig())
	}
	if err != nil {
		return nil, fmt.Errorf("provision META: %w", err)
	}
	principal, err := json.Marshal(contract.Principal{ID: admin, Name: admin})
	if err != nil {
		return nil, err
	}
	if _, err := meta.Create(ctx, contract.MetaPrincipal(admin), principal); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return nil, fmt.Errorf("seed principal %s: %w", admin, err)
	}
	m := contract.Membership{PublicKey: publicKey, Role: contract.RoleAdmin}
	for _, opt := range opts {
		opt(&m)
	}
	membership, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if _, err := meta.Create(ctx, contract.MetaMember(admin), membership); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return nil, fmt.Errorf("seed membership %s: %w", admin, err)
	}
	return meta, nil
}

// RequireRole checks the caller's registry membership against the roles a
// verb accepts. The principal is the caller's assertion — the accepted
// trust tier: outsiders cannot reach the verb at all (the account
// boundary), and member-vs-member impersonation waits for op signing by
// demand.
func RequireRole(ctx context.Context, meta jetstream.KeyValue, principal string, roles ...string) error {
	if principal == "" {
		return errors.New("principal: must not be empty")
	}
	// The tenant's own service is the operator: it holds every role and
	// no registry entry (contract.ServicePrincipal).
	if principal == contract.ServicePrincipal {
		return nil
	}
	entry, err := meta.Get(ctx, contract.MetaMember(principal))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("principal %q is not a member", principal)
	}
	if err != nil {
		return fmt.Errorf("read membership: %w", err)
	}
	var m contract.Membership
	if err := json.Unmarshal(entry.Value(), &m); err != nil {
		return fmt.Errorf("decode membership: %w", err)
	}
	if !slices.Contains(roles, m.Role) {
		return fmt.Errorf("principal %q holds role %q; one of %v required", principal, m.Role, roles)
	}
	return nil
}

// LookupByGithubID finds the one membership bound to a GitHub identity —
// the browser bridge's question (decision 0026). The registry is small by
// construction (one entry per member), so a scan is the honest index.
func LookupByGithubID(ctx context.Context, meta jetstream.KeyValue, githubID int64) (string, contract.Membership, error) {
	if githubID == 0 {
		return "", contract.Membership{}, errors.New("github id: must not be zero")
	}
	lister, err := meta.ListKeys(ctx)
	if err != nil {
		return "", contract.Membership{}, fmt.Errorf("list registry keys: %w", err)
	}
	defer func() { _ = lister.Stop() }()
	const prefix = "identity.member."
	for key := range lister.Keys() {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry, err := meta.Get(ctx, key)
		if err != nil {
			return "", contract.Membership{}, fmt.Errorf("read %s: %w", key, err)
		}
		var m contract.Membership
		if err := json.Unmarshal(entry.Value(), &m); err != nil {
			return "", contract.Membership{}, fmt.Errorf("decode %s: %w", key, err)
		}
		if m.GithubID == githubID {
			return strings.TrimPrefix(key, prefix), m, nil
		}
	}
	return "", contract.Membership{}, fmt.Errorf("no membership bound to github id %d", githubID)
}
