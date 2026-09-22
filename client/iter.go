package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// The collection reads (design 12 § the surface rule): a collection is an
// iterator — never a slice — and how it is fed is the transport rule's:
// data at rest through JetStream, a bucket scan or an ordered consumer,
// resumable and flow-controlled, never a node verb.

// scan streams a bucket's entries under the filters, in stream order,
// until the initial values are delivered: a KV watch consumed to its
// marker. metaOnly skips the values for key-only listings.
func scan(ctx context.Context, kv jetstream.KeyValue, filters []string, metaOnly bool) iter.Seq2[jetstream.KeyValueEntry, error] {
	return func(yield func(jetstream.KeyValueEntry, error) bool) {
		opts := []jetstream.WatchOpt{jetstream.IgnoreDeletes()}
		if metaOnly {
			opts = append(opts, jetstream.MetaOnly())
		}
		w, err := kv.WatchFiltered(ctx, filters, opts...)
		if err != nil {
			yield(nil, fmt.Errorf("scan %s: %w", kv.Bucket(), err))
			return
		}
		defer func() { _ = w.Stop() }()
		for {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			case entry, ok := <-w.Updates():
				if !ok || entry == nil {
					// The marker: everything at rest has been delivered.
					return
				}
				if !yield(entry, nil) {
					return
				}
			}
		}
	}
}

func fail[T any](err error) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		yield(zero, err)
	}
}

// ListLogs streams the account's logs — a META scan: the log config
// records are the authoritative inventory (0019).
func (c *Client) ListLogs(ctx context.Context) iter.Seq2[string, error] {
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return fail[string](fmt.Errorf("open META: %w", err))
	}
	return func(yield func(string, error) bool) {
		for entry, err := range scan(ctx, kv, []string{contract.MetaLogConfigPrefix + "*.config"}, true) {
			if err != nil {
				yield("", err)
				return
			}
			name := strings.TrimSuffix(strings.TrimPrefix(entry.Key(), contract.MetaLogConfigPrefix), ".config")
			if !yield(name, nil) {
				return
			}
		}
	}
}

// ListTypes streams the log's defined types — a META scan.
func (c *Client) ListTypes(ctx context.Context, log string) iter.Seq2[string, error] {
	if err := contract.ValidateLogName(log); err != nil {
		return fail[string](err)
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return fail[string](fmt.Errorf("open META: %w", err))
	}
	prefix := contract.MetaLogType(log, "")
	return func(yield func(string, error) bool) {
		for entry, err := range scan(ctx, kv, []string{prefix + "*"}, true) {
			if err != nil {
				yield("", err)
				return
			}
			if !yield(strings.TrimPrefix(entry.Key(), prefix), nil) {
				return
			}
		}
	}
}

// IndexInfo is one declared index: its name beside its declaration.
type IndexInfo struct {
	Name   string          `json:"name"`
	Kind   string          `json:"kind"`
	Config json.RawMessage `json:"config,omitempty"`
}

// ListIndexes streams the log's declared indexes — a META scan, the state
// index included (0023).
func (c *Client) ListIndexes(ctx context.Context, log string) iter.Seq2[IndexInfo, error] {
	if err := contract.ValidateLogName(log); err != nil {
		return fail[IndexInfo](err)
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return fail[IndexInfo](fmt.Errorf("open META: %w", err))
	}
	prefix := contract.MetaIndex(log, "")
	return func(yield func(IndexInfo, error) bool) {
		for entry, err := range scan(ctx, kv, []string{prefix + "*"}, false) {
			if err != nil {
				yield(IndexInfo{}, err)
				return
			}
			name := strings.TrimPrefix(entry.Key(), prefix)
			var decl contract.IndexDeclaration
			if err := json.Unmarshal(entry.Value(), &decl); err != nil {
				yield(IndexInfo{}, fmt.Errorf("decode index declaration %s: %w", name, err))
				return
			}
			if !yield(IndexInfo{Name: name, Kind: decl.Kind, Config: decl.Config}, nil) {
				return
			}
		}
	}
}

// MemberInfo is one membership: who, in which role, under which key.
type MemberInfo struct {
	Name      string `json:"name"`
	Role      string `json:"role"`
	PublicKey string `json:"public_key,omitempty"`
	GithubID  int64  `json:"github_id,omitempty"`
}

// ListMembers streams the account's registry — a META scan, any role may
// ask.
func (c *Client) ListMembers(ctx context.Context) iter.Seq2[MemberInfo, error] {
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return fail[MemberInfo](fmt.Errorf("open META: %w", err))
	}
	prefix := contract.MetaMember("")
	return func(yield func(MemberInfo, error) bool) {
		for entry, err := range scan(ctx, kv, []string{prefix + "*"}, false) {
			if err != nil {
				yield(MemberInfo{}, err)
				return
			}
			name := strings.TrimPrefix(entry.Key(), prefix)
			if name == "" {
				continue
			}
			var m contract.Membership
			if err := json.Unmarshal(entry.Value(), &m); err != nil {
				yield(MemberInfo{}, fmt.Errorf("decode membership %s: %w", name, err))
				return
			}
			if !yield(MemberInfo{Name: name, Role: m.Role, PublicKey: m.PublicKey, GithubID: m.GithubID}, nil) {
				return
			}
		}
	}
}

// ListThings streams the log's things from the state index's keys —
// derived, so possibly trailing the log; the fold watermark is excluded.
// A non-empty prefix narrows to the subtree under it.
func (c *Client) ListThings(ctx context.Context, log, prefix string) iter.Seq2[string, error] {
	if err := contract.ValidateLogName(log); err != nil {
		return fail[string](err)
	}
	kv, err := c.js.KeyValue(ctx, contract.StateBucket(log))
	if err != nil {
		return fail[string](fmt.Errorf("open state bucket: %w", err))
	}
	return func(yield func(string, error) bool) {
		for entry, err := range scan(ctx, kv, []string{">"}, true) {
			if err != nil {
				yield("", err)
				return
			}
			k := entry.Key()
			if k == contract.StateFoldKey {
				continue
			}
			if prefix != "" && k != prefix && !strings.HasPrefix(k, prefix+".") {
				continue
			}
			if !yield(k, nil) {
				return
			}
		}
	}
}

// The history reads: ordered consumers on the log's stream, resumable by
// sequence — bounded at the head observed when they started (Replay,
// FoldTail) or unbounded (Tail).

// The fetch cadence: one short wait per fetch, retried within a budget
// that outlasts any transient stall of the ordered consumer.
const (
	replayWait   = time.Second
	replayBudget = 20 * time.Second
)

// TailOpt adjusts one Tail.
type TailOpt func(*tailOpts)

type tailOpts struct {
	after uint64
	live  bool
}

// After starts the tail past a stream sequence — the exactness recipe's
// seq + 1, or wherever a reader left off.
func After(seq uint64) TailOpt {
	return func(o *tailOpts) { o.after = seq }
}

// Live starts the tail at the head: only what lands from now on.
func Live() TailOpt {
	return func(o *tailOpts) { o.live = true }
}

// Tail streams a log's ops — every thing's when thing is empty, one
// thing's otherwise — from the start by default, past a sequence with
// After, from now with Live. It never ends on its own: cancel the context
// to release the consumer. Each op carries its stream sequence, the
// cursor to resume from.
func (c *Client) Tail(ctx context.Context, log, thing string, opts ...TailOpt) iter.Seq2[contract.Op, error] {
	var o tailOpts
	for _, opt := range opts {
		opt(&o)
	}
	subject, err := opsSubject(log, thing)
	if err != nil {
		return fail[contract.Op](err)
	}
	return c.ops(ctx, log, subject, o.after, o.live, false)
}

// Replay streams a thing's full history in stream order, as of the call.
func (c *Client) Replay(ctx context.Context, log, thing string) iter.Seq2[contract.Op, error] {
	if err := contract.ValidateThing(thing); err != nil {
		return fail[contract.Op](err)
	}
	return c.ops(ctx, log, contract.OpsSubject(log, thing), 0, false, true)
}

// FoldTail streams a thing's ops after the given stream sequence, as of
// the call — the exactness recipe: read the state value, then fold the
// log from Seq+1 with contract.FoldStep.
func (c *Client) FoldTail(ctx context.Context, log, thing string, after uint64) iter.Seq2[contract.Op, error] {
	if err := contract.ValidateThing(thing); err != nil {
		return fail[contract.Op](err)
	}
	return c.ops(ctx, log, contract.OpsSubject(log, thing), after, false, true)
}

func opsSubject(log, thing string) (string, error) {
	if err := contract.ValidateLogName(log); err != nil {
		return "", err
	}
	if thing == "" {
		return contract.OpsFilter(log), nil
	}
	if err := contract.ValidateThing(thing); err != nil {
		return "", err
	}
	return contract.OpsSubject(log, thing), nil
}

// ops delivers a subject's ops through an ordered consumer: from after+1
// (or the head, live), bounded at the pending count observed at the start
// or unbounded.
func (c *Client) ops(ctx context.Context, log, subject string, after uint64, live, bounded bool) iter.Seq2[contract.Op, error] {
	return func(yield func(contract.Op, error) bool) {
		if err := contract.ValidateLogName(log); err != nil {
			yield(contract.Op{}, err)
			return
		}
		stream, err := c.js.Stream(ctx, contract.StreamName(log))
		if err != nil {
			yield(contract.Op{}, fmt.Errorf("open stream: %w", err))
			return
		}
		cfg := jetstream.OrderedConsumerConfig{FilterSubjects: []string{subject}}
		switch {
		case live:
			cfg.DeliverPolicy = jetstream.DeliverNewPolicy
		case after > 0:
			cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
			cfg.OptStartSeq = after + 1
		}
		cons, err := stream.OrderedConsumer(ctx, cfg)
		if err != nil {
			yield(contract.Op{}, fmt.Errorf("ordered consumer: %w", err))
			return
		}
		if bounded {
			c.bounded(ctx, cons, yield)
			return
		}
		c.unbounded(ctx, cons, yield)
	}
}

// bounded fetches the pending count observed at the start, in short
// fetches inside a budget, like the node's own replay: an ordered consumer
// that resets under load recovers on the next fetch, and a subject
// compacted away meanwhile reads as nothing left rather than a message
// that never came.
func (c *Client) bounded(ctx context.Context, cons jetstream.Consumer, yield func(contract.Op, error) bool) {
	info, err := cons.Info(ctx)
	if err != nil {
		yield(contract.Op{}, fmt.Errorf("consumer info: %w", err))
		return
	}
	pending := info.NumPending
	deadline := time.Now().Add(replayBudget)
	for delivered := uint64(0); delivered < pending; delivered++ {
		var msg jetstream.Msg
		for {
			var err error
			msg, err = cons.Next(jetstream.FetchMaxWait(replayWait))
			if err == nil {
				break
			}
			if !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
				yield(contract.Op{}, fmt.Errorf("replay next: %w", err))
				return
			}
			if ctx.Err() != nil {
				yield(contract.Op{}, fmt.Errorf("replay next: %w", ctx.Err()))
				return
			}
			if time.Now().After(deadline) {
				yield(contract.Op{}, fmt.Errorf("replay next: %w", err))
				return
			}
			if info, ierr := cons.Info(ctx); ierr == nil && info.NumPending == 0 {
				return
			}
		}
		op, err := parse(msg)
		if err != nil {
			yield(contract.Op{}, err)
			return
		}
		if !yield(op, nil) {
			return
		}
	}
}

// unbounded delivers until the context ends.
func (c *Client) unbounded(ctx context.Context, cons jetstream.Consumer, yield func(contract.Op, error) bool) {
	it, err := cons.Messages()
	if err != nil {
		yield(contract.Op{}, fmt.Errorf("messages: %w", err))
		return
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
		case <-stop:
		}
		it.Stop()
	}()
	for {
		msg, err := it.Next()
		if err != nil {
			if ctx.Err() != nil {
				yield(contract.Op{}, ctx.Err())
				return
			}
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				return
			}
			yield(contract.Op{}, fmt.Errorf("tail next: %w", err))
			return
		}
		op, err := parse(msg)
		if err != nil {
			yield(contract.Op{}, err)
			return
		}
		if !yield(op, nil) {
			return
		}
	}
}

func parse(msg jetstream.Msg) (contract.Op, error) {
	md, err := msg.Metadata()
	if err != nil {
		return contract.Op{}, fmt.Errorf("op metadata: %w", err)
	}
	return contract.ParseOp(msg.Subject(), md.Sequence.Stream, msg.Headers(), msg.Data()), nil
}

// Watch streams a thing's state as it stands and as it changes: the
// current value first, then every fold that moves it — a KV watch on the
// state bucket's key. It never ends on its own.
func (c *Client) Watch(ctx context.Context, log, thing string) iter.Seq2[contract.StateValue, error] {
	if err := contract.ValidateLogName(log); err != nil {
		return fail[contract.StateValue](err)
	}
	if err := contract.ValidateThing(thing); err != nil {
		return fail[contract.StateValue](err)
	}
	kv, err := c.js.KeyValue(ctx, contract.StateBucket(log))
	if err != nil {
		return fail[contract.StateValue](fmt.Errorf("open state bucket: %w", err))
	}
	return func(yield func(contract.StateValue, error) bool) {
		w, err := kv.Watch(ctx, thing, jetstream.IgnoreDeletes())
		if err != nil {
			yield(contract.StateValue{}, fmt.Errorf("watch state: %w", err))
			return
		}
		defer func() { _ = w.Stop() }()
		for {
			select {
			case <-ctx.Done():
				yield(contract.StateValue{}, ctx.Err())
				return
			case entry, ok := <-w.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue // the marker between what was at rest and what lands
				}
				var v contract.StateValue
				if err := json.Unmarshal(entry.Value(), &v); err != nil {
					yield(contract.StateValue{}, fmt.Errorf("decode state value: %w", err))
					return
				}
				if !yield(v, nil) {
					return
				}
			}
		}
	}
}

// Declaration is one of a log's declarations as a watcher sees it: a type
// record or an index declaration, its META revision, and whether this is
// its removal.
type Declaration struct {
	// Kind is "type" or "index".
	Kind     string          `json:"kind"`
	Name     string          `json:"name"`
	Revision uint64          `json:"revision"`
	Deleted  bool            `json:"deleted,omitempty"`
	Value    json.RawMessage `json:"value,omitempty"`
}

// WatchDeclarations streams a log's type records and index declarations
// as they stand and as they change — what the node and the indexers watch
// themselves, offered to a caller that keeps its own projection. It never
// ends on its own.
func (c *Client) WatchDeclarations(ctx context.Context, log string) iter.Seq2[Declaration, error] {
	if err := contract.ValidateLogName(log); err != nil {
		return fail[Declaration](err)
	}
	kv, err := c.js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return fail[Declaration](fmt.Errorf("open META: %w", err))
	}
	typePrefix := contract.MetaLogType(log, "")
	indexPrefix := contract.MetaIndex(log, "")
	return func(yield func(Declaration, error) bool) {
		w, err := kv.WatchFiltered(ctx, []string{typePrefix + "*", indexPrefix + "*"})
		if err != nil {
			yield(Declaration{}, fmt.Errorf("watch declarations: %w", err))
			return
		}
		defer func() { _ = w.Stop() }()
		for {
			select {
			case <-ctx.Done():
				yield(Declaration{}, ctx.Err())
				return
			case entry, ok := <-w.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue
				}
				d := Declaration{Revision: entry.Revision(), Deleted: entry.Operation() != jetstream.KeyValuePut}
				switch {
				case strings.HasPrefix(entry.Key(), typePrefix):
					d.Kind, d.Name = "type", strings.TrimPrefix(entry.Key(), typePrefix)
				default:
					d.Kind, d.Name = "index", strings.TrimPrefix(entry.Key(), indexPrefix)
				}
				if !d.Deleted {
					d.Value = entry.Value()
				}
				if !yield(d, nil) {
					return
				}
			}
		}
	}
}
