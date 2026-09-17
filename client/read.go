package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// ErrNoState is a state read for a thing the bucket has no entry for.
var ErrNoState = errors.New("no state for thing")

// State reads a thing's current state from the log's state bucket: a cheap
// KV read, never a fold over history at request time. The value is derived
// and may trail the log; Seq says by how much. A reader that must be exact
// calls FoldTail from Seq+1 with its own semantics.
func (c *Client) State(ctx context.Context, log, thing string) (contract.StateValue, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return contract.StateValue{}, err
	}
	if err := contract.ValidateThing(thing); err != nil {
		return contract.StateValue{}, err
	}
	kv, err := c.js.KeyValue(ctx, contract.StateBucket(log))
	if err != nil {
		return contract.StateValue{}, fmt.Errorf("open state bucket: %w", err)
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

// Replay reads a thing's full history in stream order, as of the call.
func (c *Client) Replay(ctx context.Context, log, thing string) ([]contract.Op, error) {
	if err := contract.ValidateThing(thing); err != nil {
		return nil, err
	}
	var ops []contract.Op
	err := c.fold(ctx, log, contract.OpsSubject(log, thing), 0, func(op contract.Op) error {
		ops = append(ops, op)
		return nil
	})
	return ops, err
}

// FoldTail replays a thing's ops after the given stream sequence — the
// exactness recipe: read the state value, then fold the log from Seq+1
// with the reader's own semantics.
func (c *Client) FoldTail(ctx context.Context, log, thing string, after uint64, apply func(contract.Op) error) error {
	if err := contract.ValidateThing(thing); err != nil {
		return err
	}
	return c.fold(ctx, log, contract.OpsSubject(log, thing), after, apply)
}

// fold delivers the subject's ops from after+1 through the log's current
// end, in stream order, through an ordered consumer.
func (c *Client) fold(ctx context.Context, log, subject string, after uint64, apply func(contract.Op) error) error {
	if err := contract.ValidateLogName(log); err != nil {
		return err
	}
	stream, err := c.js.Stream(ctx, contract.StreamName(log))
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
	}
	if after > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = after + 1
	}
	cons, err := stream.OrderedConsumer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("ordered consumer: %w", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return fmt.Errorf("consumer info: %w", err)
	}
	pending := info.NumPending
	for range pending {
		msg, err := cons.Next(jetstream.FetchMaxWait(10 * time.Second))
		if err != nil {
			return fmt.Errorf("replay next: %w", err)
		}
		md, err := msg.Metadata()
		if err != nil {
			return fmt.Errorf("replay metadata: %w", err)
		}
		op := contract.ParseOp(msg.Subject(), md.Sequence.Stream, msg.Headers(), msg.Data())
		if err := apply(op); err != nil {
			return err
		}
	}
	return nil
}

// ErrNoType is a type read for a name the log's vocabulary does not hold.
var ErrNoType = errors.New("type is not defined")

// GetType reads one type record from the log's vocabulary — a KV read:
// definitions are discoverable data at rest (0003, 0021), never served
// through a verb.
func (c *Client) GetType(ctx context.Context, log, name string) (contract.TypeRecord, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return contract.TypeRecord{}, err
	}
	if err := contract.ValidateTypeName(name); err != nil {
		return contract.TypeRecord{}, err
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return contract.TypeRecord{}, fmt.Errorf("open META: %w", err)
	}
	entry, err := kv.Get(ctx, contract.MetaLogType(log, name))
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

// ListTypes names the log's defined types, sorted.
func (c *Client) ListTypes(ctx context.Context, log string) ([]string, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return nil, err
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return nil, fmt.Errorf("open META: %w", err)
	}
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list META keys: %w", err)
	}
	prefix := contract.MetaLogType(log, "")
	var names []string
	for _, k := range keys {
		if name, ok := strings.CutPrefix(k, prefix); ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}
