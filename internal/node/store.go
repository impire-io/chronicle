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
	"github.com/impire-io/chronicle/internal/registry"
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

// requireRole checks the caller's registry membership against the roles a
// verb accepts — the shared check every API-serving component runs.
func (n *node) requireRole(ctx context.Context, principal string, roles ...string) error {
	return registry.RequireRole(ctx, n.meta, principal, roles...)
}

// recordSchema appends a revision: read the current one, write revision+1
// with the KV revision CAS, retrying the race. Old revisions stay readable
// in the bucket's history — recorded, never rewritten in place. The second
// return says whether the type's effect changed — a first declaration with
// an effect other than none counts, since history was folded without it.
func (n *node) recordSchema(ctx context.Context, r client.SchemaSetRequest) (uint64, bool, error) {
	key := contract.MetaLogType(r.Log, r.OpType)
	effect := contract.NormalizeEffect(r.Effect)
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := n.meta.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			value, merr := json.Marshal(contract.TypeSchema{Revision: 1, Schema: r.Schema, Effect: effect})
			if merr != nil {
				return 0, false, merr
			}
			if _, cerr := n.meta.Create(ctx, key, value); cerr != nil {
				if errors.Is(cerr, jetstream.ErrKeyExists) {
					continue // lost the first-revision race; re-read
				}
				return 0, false, cerr
			}
			return 1, effect != contract.EffectNone, nil
		}
		if err != nil {
			return 0, false, fmt.Errorf("read schema: %w", err)
		}
		var cur contract.TypeSchema
		if err := json.Unmarshal(entry.Value(), &cur); err != nil {
			return 0, false, fmt.Errorf("decode schema record: %w", err)
		}
		next := cur.Revision + 1
		value, err := json.Marshal(contract.TypeSchema{Revision: next, Schema: r.Schema, Effect: effect})
		if err != nil {
			return 0, false, err
		}
		if _, err := n.meta.Update(ctx, key, value, entry.Revision()); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				continue // stale revision; re-read
			}
			return 0, false, err
		}
		return next, contract.NormalizeEffect(cur.Effect) != effect, nil
	}
	return 0, false, errors.New("schema revision race did not settle")
}
