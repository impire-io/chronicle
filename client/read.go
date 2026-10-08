package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// ErrNoState is a state read for a thing the bucket has no entry for.
var ErrNoState = errors.New("no state for the instance")

// State reads a thing's current state from the log's state bucket: a cheap
// KV read, never a fold over history at request time. The value is derived
// and may trail the log; Seq says by how much. A reader that must be exact
// folds FoldTail from Seq+1 with contract.FoldStep — the exactness recipe.
func (c *Client) stateTail(ctx context.Context, log, thing string) (contract.StateValue, error) {
	if err := contract.ValidateStoreName(log); err != nil {
		return contract.StateValue{}, err
	}
	if err := contract.ValidateInstance(thing); err != nil {
		return contract.StateValue{}, err
	}
	kv, err := c.js.KeyValue(ctx, contract.StateBucket(log))
	if err != nil {
		return contract.StateValue{}, fmt.Errorf("cannot read state: %w", err)
	}
	entry, err := kv.Get(ctx, thing)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return contract.StateValue{}, fmt.Errorf("%w: %s in %s", ErrNoState, thing, log)
	}
	if err != nil {
		return contract.StateValue{}, fmt.Errorf("read state: %w", err)
	}
	var v contract.StateValue
	if err := json.Unmarshal(entry.Value(), &v); err != nil {
		return contract.StateValue{}, fmt.Errorf("decode state value: %w", err)
	}
	return v, nil
}

// ErrNoType is a type read for a name the log's vocabulary does not hold.
var ErrNoType = errors.New("type is not defined")

// GetType reads one type record from the log's vocabulary — a KV read:
// definitions are discoverable data at rest (0003, 0021), never served
// through a verb.
func (c *Client) GetType(ctx context.Context, log, name string) (contract.TypeRecord, error) {
	if err := contract.ValidateStoreName(log); err != nil {
		return contract.TypeRecord{}, err
	}
	if err := contract.ValidateTypeName(name); err != nil {
		return contract.TypeRecord{}, err
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return contract.TypeRecord{}, fmt.Errorf("cannot read the definitions: %w", err)
	}
	entry, err := kv.Get(ctx, contract.MetaStoreType(log, name))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return contract.TypeRecord{}, fmt.Errorf("%w: %s in %s", ErrNoType, name, log)
	}
	if err != nil {
		return contract.TypeRecord{}, fmt.Errorf("read type record: %w", err)
	}
	var rec contract.TypeRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return contract.TypeRecord{}, fmt.Errorf("decode type record: %w", err)
	}
	return rec, nil
}

// ErrNoIndex is a declaration read for an index the log does not declare.
var ErrNoIndex = errors.New("index is not declared")

// GetIndexDeclaration reads one index's declaration — the kind is what
// shapes a query (0025).
func (c *Client) GetIndexDeclaration(ctx context.Context, log, index string) (contract.IndexDeclaration, error) {
	if err := contract.ValidateStoreName(log); err != nil {
		return contract.IndexDeclaration{}, err
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return contract.IndexDeclaration{}, fmt.Errorf("cannot read the definitions: %w", err)
	}
	entry, err := kv.Get(ctx, contract.MetaIndex(log, index))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return contract.IndexDeclaration{}, fmt.Errorf("%w: %s in %s", ErrNoIndex, index, log)
	}
	if err != nil {
		return contract.IndexDeclaration{}, fmt.Errorf("read index declaration: %w", err)
	}
	var decl contract.IndexDeclaration
	if err := json.Unmarshal(entry.Value(), &decl); err != nil {
		return contract.IndexDeclaration{}, fmt.Errorf("decode index declaration: %w", err)
	}
	return decl, nil
}

// Resolve walks a thing's tail against the log's declared types (0021 § 4,
// 0022 § 2) — the same pair walk pre-flight runs, exposed so a caller can
// speak about the type before it writes.
func (c *Client) resolveTail(ctx context.Context, log, thing string) (contract.Resolution, error) {
	if err := contract.ValidateStoreName(log); err != nil {
		return contract.Resolution{}, err
	}
	if err := contract.ValidateInstance(thing); err != nil {
		return contract.Resolution{}, err
	}
	return c.types.resolve(ctx, log, thing)
}

// ErrNoStore is a store read for a name the account does not hold.
var ErrNoStore = errors.New("store does not exist")

// GetStore reads one store's settings — its history policy, description
// and size limit — a KV read of data at rest.
func (c *Client) GetStore(ctx context.Context, store string) (contract.StoreConfig, error) {
	if err := contract.ValidateStoreName(store); err != nil {
		return contract.StoreConfig{}, err
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return contract.StoreConfig{}, fmt.Errorf("cannot read the definitions: %w", err)
	}
	entry, err := kv.Get(ctx, contract.MetaStoreConfig(store))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return contract.StoreConfig{}, fmt.Errorf("%w: %s", ErrNoStore, store)
	}
	if err != nil {
		return contract.StoreConfig{}, fmt.Errorf("read store %s: %w", store, err)
	}
	var cfg contract.StoreConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		return contract.StoreConfig{}, fmt.Errorf("decode store %s: %w", store, err)
	}
	return cfg, nil
}
