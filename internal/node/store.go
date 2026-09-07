package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// listLogs reads the authoritative log inventory: every log.<log>.config
// key in META.
func (n *node) listLogs(ctx context.Context) ([]string, error) {
	keys, err := n.meta.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list META keys: %w", err)
	}
	var logs []string
	for _, k := range keys {
		name, ok := strings.CutPrefix(k, contract.MetaLogConfigPrefix)
		if !ok {
			continue
		}
		name, ok = strings.CutSuffix(name, ".config")
		if !ok || strings.Contains(name, ".") {
			continue
		}
		logs = append(logs, name)
	}
	return logs, nil
}

// requireRole checks the caller's registry membership. The principal is the
// caller's assertion — the accepted trust tier: outsiders cannot reach the
// verb at all (the account boundary), and member-vs-member impersonation
// waits for op signing by demand.
func (n *node) requireRole(ctx context.Context, principal, role string) error {
	if principal == "" {
		return errors.New("principal: must not be empty")
	}
	entry, err := n.meta.Get(ctx, contract.MetaMember(principal))
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
	if m.Role != role {
		return fmt.Errorf("principal %q holds role %q; %q required", principal, m.Role, role)
	}
	return nil
}

// recordSchema appends a revision: read the current one, write revision+1
// with the KV revision CAS, retrying the race. Old revisions stay readable
// in the bucket's history — recorded, never rewritten in place.
func (n *node) recordSchema(ctx context.Context, r client.SchemaSetRequest) (uint64, error) {
	key := contract.MetaLogType(r.Log, r.OpType)
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := n.meta.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			value, merr := json.Marshal(contract.TypeSchema{Revision: 1, Schema: r.Schema})
			if merr != nil {
				return 0, merr
			}
			if _, cerr := n.meta.Create(ctx, key, value); cerr != nil {
				if errors.Is(cerr, jetstream.ErrKeyExists) {
					continue // lost the first-revision race; re-read
				}
				return 0, cerr
			}
			return 1, nil
		}
		if err != nil {
			return 0, fmt.Errorf("read schema: %w", err)
		}
		var cur contract.TypeSchema
		if err := json.Unmarshal(entry.Value(), &cur); err != nil {
			return 0, fmt.Errorf("decode schema record: %w", err)
		}
		next := cur.Revision + 1
		value, err := json.Marshal(contract.TypeSchema{Revision: next, Schema: r.Schema})
		if err != nil {
			return 0, err
		}
		if _, err := n.meta.Update(ctx, key, value, entry.Revision()); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				continue // stale revision; re-read
			}
			return 0, err
		}
		return next, nil
	}
	return 0, errors.New("schema revision race did not settle")
}
