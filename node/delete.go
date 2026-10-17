package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// The two deletes of decision 0062. A store's deletion is total and runs
// in a fixed order a second run finishes from any step of; a type's
// deletion is a declaration change — latest declaration wins — that
// leaves its instances untyped, their history whole, until the type is
// defined again.

// handleStoreDelete serves LOG.DELETE: admin role, a name that is a store,
// and then the fixed order below. A store already mid-deletion is
// finished, not refused — the second run is how a crashed first one ends.
func (n *node) handleStoreDelete(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var r client.StoreDeleteRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error(contract.CodeBadRequest, err.Error(), nil)
		return
	}
	if err := n.requireCaller(ctx, req, contract.RoleAdmin); err != nil {
		return
	}
	if err := contract.ValidateStoreName(r.Store); err != nil {
		_ = req.Error(contract.CodeBadStoreName, err.Error(), nil)
		return
	}
	if _, err := n.storeConfig(ctx, r.Store); errors.Is(err, jetstream.ErrKeyNotFound) {
		_ = req.Error(contract.CodeNoSuchStore, fmt.Sprintf("store %q does not exist", r.Store), nil)
		return
	} else if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	if err := n.deleteStore(ctx, r.Store); err != nil {
		_ = req.Error(contract.CodeInternal, fmt.Sprintf("delete store %s: %v", r.Store, err), nil)
		return
	}
	reply, err := json.Marshal(client.StoreDeleteResponse{Deleted: true})
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// deleteStore removes a store in the order 03-meta-and-state.md § store
// configuration states: the config's status goes to deleting — the claim,
// by compare-and-set, from which every other verb on the store refuses —
// then the fold stops, every index declaration but state goes (its
// workload with it), the state declaration goes, the state bucket, the
// stream with every operation and schedule in it, the type records, and
// the config key last. Every step is idempotent, so a run that finds a
// step already done moves on: a second call, or a replica's boot,
// finishes what a first one left. A store whose config is already gone
// is deleted.
func (n *node) deleteStore(ctx context.Context, store string) error {
	mu := n.storeMutex(store)
	mu.Lock()
	defer mu.Unlock()

	if err := n.claimDeleting(ctx, store); err != nil {
		return err
	}
	n.stopFold(store)

	keys, err := n.meta.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return fmt.Errorf("list META keys: %w", err)
	}
	indexPrefix := contract.MetaIndex(store, "")
	typePrefix := contract.MetaStoreType(store, "")
	var indexes, types []string
	for _, k := range keys {
		if name, ok := strings.CutPrefix(k, indexPrefix); ok {
			indexes = append(indexes, name)
		}
		if name, ok := strings.CutPrefix(k, typePrefix); ok {
			types = append(types, name)
		}
	}
	for _, index := range indexes {
		if index == contract.StateIndexName {
			continue
		}
		if err := n.meta.Delete(ctx, contract.MetaIndex(store, index)); err != nil {
			return fmt.Errorf("remove index %s: %w", index, err)
		}
		if n.indexes != nil {
			if err := n.indexes.IndexDeleted(ctx, store, index); err != nil {
				n.logger.Warn("index removed with its store but the report failed; the scan retires the workload when the record catches up", "log", store, "index", index, "err", err)
			}
		}
	}
	if err := n.meta.Delete(ctx, contract.MetaIndex(store, contract.StateIndexName)); err != nil {
		return fmt.Errorf("remove state declaration: %w", err)
	}
	if err := n.js.DeleteKeyValue(ctx, contract.StateBucket(store)); err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("delete state bucket: %w", err)
	}
	if err := n.js.DeleteStream(ctx, contract.StreamName(store)); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("delete stream: %w", err)
	}
	for _, typeName := range types {
		if err := n.meta.Delete(ctx, contract.MetaStoreType(store, typeName)); err != nil {
			return fmt.Errorf("remove type %s: %w", typeName, err)
		}
	}
	if err := n.meta.Delete(ctx, contract.MetaStoreConfig(store)); err != nil {
		return fmt.Errorf("remove store configuration: %w", err)
	}
	n.mu.Lock()
	delete(n.rolls, store)
	n.mu.Unlock()
	return nil
}

// claimDeleting moves the store's config to status deleting under the
// KV revision, racing another claimant fairly: whoever lands first
// claims, and the other sees the claim and carries on. A config already
// gone is a delete that finished its last step.
func (n *node) claimDeleting(ctx context.Context, store string) error {
	for {
		entry, err := n.meta.Get(ctx, contract.MetaStoreConfig(store))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read store configuration: %w", err)
		}
		var cfg contract.StoreConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
			return fmt.Errorf("decode store configuration: %w", err)
		}
		if cfg.Status == contract.StoreStatusDeleting {
			return nil
		}
		cfg.Status = contract.StoreStatusDeleting
		value, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		_, err = n.meta.Update(ctx, contract.MetaStoreConfig(store), value, entry.Revision())
		if err == nil {
			return nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return fmt.Errorf("mark store deleting: %w", err)
		}
	}
}

// stopFold stops a store's fold if one runs, and forgets it.
func (n *node) stopFold(store string) {
	n.mu.Lock()
	run := n.folds[store]
	delete(n.folds, store)
	n.mu.Unlock()
	if run != nil {
		run.stop()
	}
}

// handleTypeDelete serves TYPE.DELETE: the record goes, and the store's
// derived state is rebuilt under the declarations that remain — every
// instance that resolved through the type is untyped from then on, its
// history kept (decision 0062).
func (n *node) handleTypeDelete(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.TypeDeleteRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error(contract.CodeBadRequest, err.Error(), nil)
		return
	}
	if err := n.requireCaller(ctx, req, contract.RoleAdmin); err != nil {
		return
	}
	if err := contract.ValidateStoreName(r.Store); err != nil {
		_ = req.Error(contract.CodeBadStoreName, err.Error(), nil)
		return
	}
	if err := contract.ValidateTypeName(r.Type); err != nil {
		_ = req.Error(contract.CodeBadTypeName, err.Error(), nil)
		return
	}
	if !n.requireStore(ctx, req, r.Store) {
		return
	}
	// Get first: deleting an absent key succeeds silently in KV, and the
	// caller deserves the honest answer.
	if _, err := n.meta.Get(ctx, contract.MetaStoreType(r.Store, r.Type)); err != nil {
		_ = req.Error(contract.CodeNoSuchType, fmt.Sprintf("type %q is not defined in store %q", r.Type, r.Store), nil)
		return
	}
	if err := n.meta.Delete(ctx, contract.MetaStoreType(r.Store, r.Type)); err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	// Latest declaration wins: the store's derived state is suspect and
	// is rebuilt by replay under what remains (0011 § 3, 0021 § 5).
	if err := n.rebuildStore(ctx, r.Store); err != nil {
		_ = req.Error(contract.CodeInternal, fmt.Sprintf("rebuild state: %v", err), nil)
		return
	}
	reply, err := json.Marshal(client.TypeDeleteResponse{Deleted: true})
	if err != nil {
		_ = req.Error(contract.CodeInternal, err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}
