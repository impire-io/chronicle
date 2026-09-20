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

// ErrNoIndex is a declaration read for an index the log does not declare.
var ErrNoIndex = errors.New("index is not declared")

// ListLogs names the account's logs, sorted — a META read: the log
// config records are the authoritative inventory (0019).
func (c *Client) ListLogs(ctx context.Context) ([]string, error) {
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
	var names []string
	for _, k := range keys {
		rest, ok := strings.CutPrefix(k, contract.MetaLogConfigPrefix)
		if !ok {
			continue
		}
		name, ok := strings.CutSuffix(rest, ".config")
		// log names admit no dot, so log.<log>.type.<t> never matches.
		if !ok || strings.Contains(name, ".") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// IndexInfo is one declared index: its name beside its declaration.
type IndexInfo struct {
	Name   string
	Kind   string
	Config json.RawMessage
}

// ListIndexes reads the log's declared indexes from META, sorted — the
// state index included (0023).
func (c *Client) ListIndexes(ctx context.Context, log string) ([]IndexInfo, error) {
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
	prefix := contract.MetaIndex(log, "")
	var infos []IndexInfo
	for _, k := range keys {
		name, ok := strings.CutPrefix(k, prefix)
		if !ok || strings.Contains(name, ".") {
			continue
		}
		entry, err := kv.Get(ctx, k)
		if err != nil {
			return nil, fmt.Errorf("read index declaration %s: %w", name, err)
		}
		var decl contract.IndexDeclaration
		if err := json.Unmarshal(entry.Value(), &decl); err != nil {
			return nil, fmt.Errorf("decode index declaration %s: %w", name, err)
		}
		infos = append(infos, IndexInfo{Name: name, Kind: decl.Kind, Config: decl.Config})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

// GetIndexDeclaration reads one index's declaration — the kind is what
// shapes a query (0025).
func (c *Client) GetIndexDeclaration(ctx context.Context, log, index string) (contract.IndexDeclaration, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return contract.IndexDeclaration{}, err
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return contract.IndexDeclaration{}, fmt.Errorf("open META: %w", err)
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

// MemberInfo is one membership: who, in which role, under which key.
type MemberInfo struct {
	Name      string
	Role      string
	PublicKey string
	GithubID  int64
}

// ListMembers reads the tenant's registry from META, sorted by name — a
// read of data at rest, any role may ask.
func (c *Client) ListMembers(ctx context.Context) ([]MemberInfo, error) {
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
	prefix := contract.MetaMember("")
	var members []MemberInfo
	for _, k := range keys {
		name, ok := strings.CutPrefix(k, prefix)
		if !ok || name == "" {
			continue
		}
		entry, err := kv.Get(ctx, k)
		if err != nil {
			return nil, fmt.Errorf("read membership %s: %w", name, err)
		}
		var m contract.Membership
		if err := json.Unmarshal(entry.Value(), &m); err != nil {
			return nil, fmt.Errorf("decode membership %s: %w", name, err)
		}
		members = append(members, MemberInfo{Name: name, Role: m.Role, PublicKey: m.PublicKey, GithubID: m.GithubID})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	return members, nil
}

// ListThings names the log's things from the state index's keys, sorted —
// derived, so possibly trailing the log; the fold watermark is excluded.
// A non-empty prefix filters to the subtree under it.
func (c *Client) ListThings(ctx context.Context, log, prefix string) ([]string, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return nil, err
	}
	kv, err := c.js.KeyValue(ctx, contract.StateBucket(log))
	if err != nil {
		return nil, fmt.Errorf("open state bucket: %w", err)
	}
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list state keys: %w", err)
	}
	var names []string
	for _, k := range keys {
		if k == contract.StateFoldKey {
			continue
		}
		if prefix != "" && k != prefix && !strings.HasPrefix(k, prefix+".") {
			continue
		}
		names = append(names, k)
	}
	sort.Strings(names)
	return names, nil
}

// Resolve walks a thing's tail against the log's declared types (0021 § 4,
// 0022 § 2) — the same pair walk pre-flight runs, exposed so a caller can
// speak about the type before it writes.
func (c *Client) Resolve(ctx context.Context, log, thing string) (contract.Resolution, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return contract.Resolution{}, err
	}
	if err := contract.ValidateThing(thing); err != nil {
		return contract.Resolution{}, err
	}
	return c.types.resolve(ctx, log, thing)
}
