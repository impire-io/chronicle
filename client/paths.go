package client

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"

	"github.com/impire-io/chronicle/contract"
)

// The instance surface in the user's words (chronicle-hq decision 0044,
// design 07): every method here names an instance by its path —
// type/id, with /name/id for each child — and converts it to the tail
// the wire stores once, at this edge. Subjects, keys and the fold never
// see a slash; a user never types a dot.

// CreateFromSnapshot creates an instance by publishing its first snapshot
// with the create-only guard — the untyped form, and the application's
// own when it folds state itself. A typed instance is created with Create.
func (c *Client) CreateFromSnapshot(ctx context.Context, store, path string, state json.RawMessage, opts ...ApplyOpt) (Ack, error) {
	tail, err := contract.PathTail(path)
	if err != nil {
		return Ack{}, err
	}
	return c.createFromSnapshotTail(ctx, store, tail, state, opts...)
}

// Create creates an instance by applying one of its type's operations —
// `create` by convention — with the create-only guard: the data is
// checked against that operation's schema, and the write lands only if
// the instance does not exist yet. A retried create whose operation
// already landed reports success; a create of an instance someone else
// made first reports ErrInstanceExists.
func (c *Client) Create(ctx context.Context, store, path, op string, data []byte, opts ...ApplyOpt) (Ack, error) {
	tail, err := contract.PathTail(path)
	if err != nil {
		return Ack{}, err
	}
	return c.createTail(ctx, store, tail, op, data, opts...)
}

// Apply applies an operation to an instance — a direct JetStream publish,
// nothing in between. The data is checked against the operation's schema
// before it is sent; WithExpectedSeq carries the expected sequence
// (0018), and a miss is ErrInstanceMoved: re-read, re-validate, retry.
func (c *Client) Apply(ctx context.Context, store, path, op string, data []byte, opts ...ApplyOpt) (Ack, error) {
	tail, err := contract.PathTail(path)
	if err != nil {
		return Ack{}, err
	}
	return c.applyTail(ctx, store, tail, op, data, opts...)
}

// SaveSnapshot publishes an application-materialised snapshot that
// replaces the instance's history in one write — the application's own
// compaction, guarded at upTo (the sequence of the last operation the
// state covers). ErrStaleVersion says the history moved past it.
func (c *Client) SaveSnapshot(ctx context.Context, store, path string, state json.RawMessage, frontier []string, upTo uint64, opts ...ApplyOpt) (Ack, error) {
	tail, err := contract.PathTail(path)
	if err != nil {
		return Ack{}, err
	}
	return c.saveSnapshotTail(ctx, store, tail, state, frontier, upTo, opts...)
}

// Snapshot asks the node to take a snapshot of the instance now: write
// its current state as one entry and compact the history before it. A
// response with Taken false is the node declining — a full-history
// policy, nothing to compact, a lost race — with the reason; only
// transport and refusal failures are errors.
func (c *Client) Snapshot(ctx context.Context, store, path string) (InstanceSnapshotResponse, error) {
	if _, err := contract.PathTail(path); err != nil {
		return InstanceSnapshotResponse{}, err
	}
	return c.snapshotTail(ctx, store, path)
}

// State reads an instance's current state: a KV read, never a fold over
// history at request time. Seq says which operation it stands at.
func (c *Client) State(ctx context.Context, store, path string) (contract.StateValue, error) {
	tail, err := contract.PathTail(path)
	if err != nil {
		return contract.StateValue{}, err
	}
	return c.stateTail(ctx, store, tail)
}

// History streams an instance's operations in order, as of the call.
func (c *Client) History(ctx context.Context, store, path string) iter.Seq2[contract.Op, error] {
	tail, err := contract.PathTail(path)
	if err != nil {
		return fail[contract.Op](err)
	}
	return c.historyTail(ctx, store, tail)
}

// FoldTail streams an instance's operations after the given sequence, as
// of the call — the exactness recipe: read the state value, then fold
// from Seq+1 with contract.FoldStep.
func (c *Client) FoldTail(ctx context.Context, store, path string, after uint64) iter.Seq2[contract.Op, error] {
	tail, err := contract.PathTail(path)
	if err != nil {
		return fail[contract.Op](err)
	}
	return c.foldTailOf(ctx, store, tail, after)
}

// Tail streams a store's operations — every instance's when path is
// empty, one instance's otherwise — from the start by default, past a
// sequence with After, from now with Live. It never ends on its own.
func (c *Client) Tail(ctx context.Context, store, path string, opts ...TailOpt) iter.Seq2[contract.Op, error] {
	tail := ""
	if path != "" {
		var err error
		if tail, err = contract.PathTail(path); err != nil {
			return fail[contract.Op](err)
		}
	}
	return c.tailOps(ctx, store, tail, opts...)
}

// Watch streams an instance's state as it stands and as it changes: the
// current value first, then every change. It never ends on its own.
func (c *Client) Watch(ctx context.Context, store, path string) iter.Seq2[contract.StateValue, error] {
	tail, err := contract.PathTail(path)
	if err != nil {
		return fail[contract.StateValue](err)
	}
	return c.watchTail(ctx, store, tail)
}

// Resolve walks a path against the store's defined types (0021 § 4,
// 0022 § 2) — the same walk pre-flight runs, exposed so a caller can
// speak about the type before it writes.
func (c *Client) Resolve(ctx context.Context, store, path string) (contract.Resolution, error) {
	tail, err := contract.PathTail(path)
	if err != nil {
		return contract.Resolution{}, err
	}
	return c.resolveTail(ctx, store, tail)
}

// InstanceInfo is one instance as a listing yields it: its path, the type
// its path names, the sequence its state stands at, and the state.
type InstanceInfo struct {
	Path  string          `json:"path"`
	Type  string          `json:"type"`
	Seq   uint64          `json:"seq"`
	State json.RawMessage `json:"state,omitempty"`
}

// ListOpt narrows a listing.
type ListOpt func(*listOpts)

type listOpts struct {
	typeName string
	under    string
	where    [][2]string
}

// ByType keeps the instances of one type: top-level instances whose path
// starts with it, or — under a parent — the children whose declared child
// type it is.
func ByType(typeName string) ListOpt {
	return func(o *listOpts) { o.typeName = typeName }
}

// Under keeps an instance's direct children, by path.
func Under(path string) ListOpt {
	return func(o *listOpts) { o.under = path }
}

// Where keeps instances whose state field equals the value — a scalar
// equality on the folded state (strings, numbers and booleans compared
// by their text), applied client-side. Repeatable; every clause must
// hold.
func Where(field, value string) ListOpt {
	return func(o *listOpts) { o.where = append(o.where, [2]string{field, value}) }
}

// ListInstances streams a store's instances from its state — derived, so
// possibly trailing the history. Without options every instance at every
// level streams; ByType, Under and Where narrow it. A bucket scan: nothing
// is added to the wire, and the filter is this client's (decision 0045).
func (c *Client) ListInstances(ctx context.Context, store string, opts ...ListOpt) iter.Seq2[InstanceInfo, error] {
	var o listOpts
	for _, opt := range opts {
		opt(&o)
	}
	if err := contract.ValidateStoreName(store); err != nil {
		return fail[InstanceInfo](err)
	}
	var underTail string
	if o.under != "" {
		var err error
		if underTail, err = contract.PathTail(o.under); err != nil {
			return fail[InstanceInfo](err)
		}
	}
	// Under a parent, a type narrows by the parent's children map: the
	// child names whose declared type is the one asked for.
	var childNames map[string]bool
	if o.under != "" && o.typeName != "" {
		res, err := c.resolveTail(ctx, store, underTail)
		if err != nil {
			return fail[InstanceInfo](err)
		}
		childNames = map[string]bool{}
		if res.Kind == contract.ResolvedTyped {
			for name, t := range res.Record.Children {
				if t == o.typeName {
					childNames[name] = true
				}
			}
		}
	}
	kv, err := c.js.KeyValue(ctx, contract.StateBucket(store))
	if err != nil {
		return fail[InstanceInfo](fmt.Errorf("cannot read the instances of store %s: %w", store, err))
	}
	needValues := len(o.where) > 0
	return func(yield func(InstanceInfo, error) bool) {
		for entry, err := range scan(ctx, kv, []string{">"}, false) {
			if err != nil {
				yield(InstanceInfo{}, err)
				return
			}
			tail := entry.Key()
			if tail == contract.StateFoldKey {
				continue
			}
			toks := strings.Split(tail, contract.TailSeparator)
			switch {
			case o.under != "":
				// A direct child: the parent's tail, then exactly name.id.
				if !strings.HasPrefix(tail, underTail+contract.TailSeparator) || len(toks) != len(strings.Split(underTail, contract.TailSeparator))+2 {
					continue
				}
				if childNames != nil && !childNames[toks[len(toks)-2]] {
					continue
				}
			case o.typeName != "":
				// A top-level instance of the type: exactly type.id.
				if len(toks) != 2 || toks[0] != o.typeName {
					continue
				}
			}
			info := InstanceInfo{Path: contract.TailPath(tail), Type: toks[0]}
			if o.under != "" {
				info.Type = childTypeOf(childNames, toks, o.typeName)
			}
			var sv contract.StateValue
			if err := json.Unmarshal(entry.Value(), &sv); err != nil {
				yield(InstanceInfo{}, fmt.Errorf("decode the state of %s: %w", info.Path, err))
				return
			}
			info.Seq, info.State = sv.Seq, sv.State
			if needValues && !matches(sv.State, o.where) {
				continue
			}
			if !yield(info, nil) {
				return
			}
		}
	}
}

// childTypeOf names a child's type in a listing under a parent: the type
// asked for when one was, else the child's name — the grammar's own
// answer when the parent's record was not consulted.
func childTypeOf(childNames map[string]bool, toks []string, asked string) string {
	if asked != "" && childNames != nil {
		return asked
	}
	return toks[len(toks)-2]
}

// matches applies Where clauses to a state document: every named field
// must exist and render to the value's text.
func matches(state json.RawMessage, where [][2]string) bool {
	var doc map[string]any
	if err := json.Unmarshal(state, &doc); err != nil {
		return false
	}
	for _, clause := range where {
		v, ok := lookupField(doc, clause[0])
		if !ok {
			return false
		}
		switch x := v.(type) {
		case string:
			if x != clause[1] {
				return false
			}
		case bool, float64, json.Number:
			if fmt.Sprint(x) != clause[1] {
				return false
			}
		case nil:
			if clause[1] != "null" {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// lookupField walks a dotted field path through nested objects.
func lookupField(doc map[string]any, field string) (any, bool) {
	var cur any = doc
	for _, seg := range strings.Split(field, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}
