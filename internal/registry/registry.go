// Package registry reads the identity slice of META — the membership
// registry of decision 0007 — for the role checks that gate the
// CHRON.API.> surface (04-fleet.md). Every API-serving component runs the
// same check; a component carrying its own copy is a drift hazard.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// RequireRole checks the caller's registry membership against the roles a
// verb accepts. The principal is the caller's assertion — the accepted
// trust tier: outsiders cannot reach the verb at all (the account
// boundary), and member-vs-member impersonation waits for op signing by
// demand.
func RequireRole(ctx context.Context, meta jetstream.KeyValue, principal string, roles ...string) error {
	if principal == "" {
		return errors.New("principal: must not be empty")
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
