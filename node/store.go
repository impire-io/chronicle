package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/foldcore"
	"github.com/impire-io/chronicle/registry"
)

// listStores reads the authoritative log inventory: every log.<log>.config
// key in META.
func (n *node) listStores(ctx context.Context) ([]string, error) {
	keys, err := n.meta.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list META keys: %w", err)
	}
	var logs []string
	for _, k := range keys {
		name, ok := strings.CutPrefix(k, contract.MetaStoreConfigPrefix)
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

// storeConfig reads log.<store>.config: jetstream.ErrKeyNotFound is no
// such store.
func (n *node) storeConfig(ctx context.Context, store string) (contract.StoreConfig, error) {
	entry, err := n.meta.Get(ctx, contract.MetaStoreConfig(store))
	if err != nil {
		return contract.StoreConfig{}, err
	}
	var cfg contract.StoreConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		return contract.StoreConfig{}, fmt.Errorf("decode store configuration: %w", err)
	}
	return cfg, nil
}

// requireStore answers a verb on a store that does not exist, or is being
// deleted (decision 0062), with the refusal; true means the store is
// there to act on.
func (n *node) requireStore(ctx context.Context, req micro.Request, store string) bool {
	cfg, err := n.storeConfig(ctx, store)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		_ = req.Error(contract.CodeNoSuchStore, fmt.Sprintf("store %q does not exist", store), nil)
		return false
	}
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return false
	}
	if cfg.Status == contract.StoreStatusDeleting {
		_ = req.Error(contract.CodeStoreDeleting, fmt.Sprintf("store %q is being deleted", store), nil)
		return false
	}
	return true
}

// requireRole asks the node's resolver for the caller's role (decision
// 0061) and checks it against the roles a verb accepts — the shared check
// every API-serving component runs.
func (n *node) requireRole(ctx context.Context, principal string, roles ...string) error {
	return registry.Require(ctx, n.roles, principal, roles...)
}

// requireCaller reads the caller from the request's subject — the name
// the server bound to the connection (0057) — and checks its role,
// answering the refusal itself; the handler returns on a non-nil error.
func (n *node) requireCaller(ctx context.Context, req micro.Request, roles ...string) error {
	caller, err := contract.CallerFromSubject(req.Subject())
	if err != nil {
		_ = req.Error(contract.CodeBadRequest, err.Error(), nil)
		return err
	}
	if err := n.requireRole(ctx, caller, roles...); err != nil {
		_ = req.Error(registry.Code(err), err.Error(), nil)
		return err
	}
	return nil
}

// recordAttester writes the node's public key to META, create-if-absent:
// the account's attester (0057), which no member can write and every
// verifier reads. A second replica with the same credential finds it there.
func (n *node) recordAttester(ctx context.Context) error {
	pub, err := n.signer.PublicKey()
	if err != nil {
		return fmt.Errorf("attester public key: %w", err)
	}
	value, err := json.Marshal(contract.Attester{PublicKey: pub})
	if err != nil {
		return err
	}
	if _, err := n.meta.Create(ctx, contract.MetaAttester(pub), value); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return fmt.Errorf("record attester key: %w", err)
	}
	n.attester = pub
	return nil
}

// recordType appends a type revision: read the current record, write
// revision+1 with the KV revision CAS, retrying the race. Old revisions
// stay readable in the bucket's history — recorded, never rewritten in
// place. The second return says whether the record's fold fingerprint
// changed — history, aspects, or an operation's effect — making the log's
// derived state suspect.
func (n *node) recordType(ctx context.Context, r client.TypeDefineRequest) (uint64, bool, error) {
	key := contract.MetaStoreType(r.Store, r.Type)
	next := contract.TypeRecord{
		Schema:     r.Schema,
		History:    r.History,
		Children:   r.Children,
		Operations: make(map[string]contract.OpDef, len(r.Operations)),
	}
	for op, def := range r.Operations {
		def.Effect = contract.NormalizeEffect(def.Effect)
		next.Operations[op] = def
	}
	if len(next.Operations) == 0 {
		next.Operations = nil
	}
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := n.meta.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			next.Revision = 1
			value, merr := json.Marshal(next)
			if merr != nil {
				return 0, false, merr
			}
			if _, cerr := n.meta.Create(ctx, key, value); cerr != nil {
				if errors.Is(cerr, jetstream.ErrKeyExists) {
					continue // lost the first-revision race; re-read
				}
				return 0, false, cerr
			}
			return 1, foldcore.FoldFingerprint(&next) != foldcore.FoldFingerprint(nil), nil
		}
		if err != nil {
			return 0, false, fmt.Errorf("read type record: %w", err)
		}
		var cur contract.TypeRecord
		if err := json.Unmarshal(entry.Value(), &cur); err != nil {
			return 0, false, fmt.Errorf("decode type record: %w", err)
		}
		next.Revision = cur.Revision + 1
		value, err := json.Marshal(next)
		if err != nil {
			return 0, false, err
		}
		if _, err := n.meta.Update(ctx, key, value, entry.Revision()); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				continue // stale revision; re-read
			}
			return 0, false, err
		}
		return next.Revision, foldcore.FoldFingerprint(&cur) != foldcore.FoldFingerprint(&next), nil
	}
	return 0, false, errors.New("type revision race did not settle")
}
