package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// foldRun is one log's running fold; stop is idempotent and returns only
// when consumption has fully ceased.
type foldRun struct {
	f    *fold
	stop func()
}

// startFold runs one log's fold: an ordered consumer over the ops family,
// one fold state per subject, never letting one subject's op touch
// another's. Idempotent per log.
func (n *node) startFold(ctx context.Context, log string) error {
	n.mu.Lock()
	if _, ok := n.folds[log]; ok {
		n.mu.Unlock()
		return nil
	}
	n.mu.Unlock()

	stream, err := n.js.Stream(ctx, contract.StreamName(log))
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	states, err := n.js.KeyValue(ctx, contract.StateBucket(log))
	if err != nil {
		return fmt.Errorf("open state bucket: %w", err)
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{contract.OpsFilter(log)},
	})
	if err != nil {
		return fmt.Errorf("ordered consumer: %w", err)
	}

	f := &fold{node: n, log: log, states: states, active: map[string]struct{}{}}
	cc, err := cons.Consume(f.apply)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	var once sync.Once
	run := &foldRun{f: f, stop: func() {
		once.Do(cc.Stop)
		<-cc.Closed()
	}}
	n.mu.Lock()
	n.folds[log] = run
	n.mu.Unlock()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		<-n.foldCtx.Done()
		run.stop()
	}()
	return nil
}

// rebuildLog is the §6.2 rebuild, mechanized: the log's derived state is
// suspect (an effect changed), so stop the fold, purge the bucket, and
// re-fold the whole stream under the current declarations — latest
// declaration wins (decision 0011). It holds the log's derived-state
// mutex so no rollup publishes a snapshot computed mid-change.
func (n *node) rebuildLog(ctx context.Context, log string) error {
	mu := n.logMutex(log)
	mu.Lock()
	defer mu.Unlock()

	n.mu.Lock()
	run := n.folds[log]
	delete(n.folds, log)
	n.mu.Unlock()
	if run != nil {
		run.stop()
	}
	states, err := n.js.KeyValue(ctx, contract.StateBucket(log))
	if err != nil {
		return fmt.Errorf("open state bucket: %w", err)
	}
	keys, err := states.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return fmt.Errorf("list state keys: %w", err)
	}
	for _, k := range keys {
		if err := states.Purge(ctx, k); err != nil {
			return fmt.Errorf("purge %s: %w", k, err)
		}
	}
	return n.startFold(ctx, log)
}

type fold struct {
	node   *node
	log    string
	states jetstream.KeyValue

	mu sync.Mutex
	// active is the timer trigger's feed: every subject this fold saw an
	// op on since the last sweep (04-fleet.md § the node's duties). A
	// restart or rebuild re-folds the stream, so everything re-enters —
	// the next sweep is a full one, by design.
	active map[string]struct{}
}

// swapActive hands the sweep the active set and starts a fresh one.
func (f *fold) swapActive() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	things := make([]string, 0, len(f.active))
	for thing := range f.active {
		things = append(things, thing)
	}
	f.active = map[string]struct{}{}
	return things
}

// apply folds one message. Ordered consumers redeliver on gaps, so apply
// stays idempotent: the state CAS skips anything at or below the stored
// seq.
func (f *fold) apply(msg jetstream.Msg) {
	md, err := msg.Metadata()
	if err != nil {
		f.node.logger.Warn("fold: message without metadata", "log", f.log, "err", err)
		return
	}
	op := contract.ParseOp(msg.Subject(), md.Sequence.Stream, msg.Headers(), msg.Data())
	thing := contract.ThingFromSubject(f.log, op.Subject)
	if thing == op.Subject {
		// Not the ops family; other in-stream families ride the same
		// stream and the same replay, untouched by the fold.
		return
	}
	f.mu.Lock()
	f.active[thing] = struct{}{}
	f.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if op.Type == contract.OpTypeSnapshot {
		snap, err := contract.ParseSnapshot(op.Payload)
		if err != nil {
			f.node.logger.Warn("fold: marked malformed snapshot", "log", f.log, "thing", thing, "op", op.ID, "err", err)
			return
		}
		f.resetState(ctx, thing, op.Seq, snap.State)
		return
	}
	f.applyEffect(ctx, op, thing)
}

// applyEffect is the fold's rules for a non-snapshot op (decision 0011):
// unknown types warn; a schema-invalid op of a known type is marked and
// takes no effect; effect none and unknown effect values move nothing;
// effect merge applies the payload as an RFC 7386 merge patch.
func (f *fold) applyEffect(ctx context.Context, op contract.Op, thing string) {
	entry, err := f.node.meta.Get(ctx, contract.MetaLogType(f.log, op.Type))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		f.node.logger.Warn("fold: unknown op type ignored", "log", f.log, "thing", thing, "op", op.ID, "type", op.Type)
		return
	}
	if err != nil {
		f.node.logger.Warn("fold: read type record", "log", f.log, "type", op.Type, "err", err)
		return
	}
	var ts contract.TypeSchema
	if err := json.Unmarshal(entry.Value(), &ts); err != nil {
		f.node.logger.Warn("fold: decode type record", "log", f.log, "type", op.Type, "err", err)
		return
	}
	sch, err := client.CompileSchema(ts.Schema)
	if err != nil {
		f.node.logger.Warn("fold: compile schema", "log", f.log, "type", op.Type, "err", err)
		return
	}
	var v any
	if err := json.Unmarshal(op.Payload, &v); err != nil {
		f.node.logger.Warn("fold: marked invalid payload", "log", f.log, "thing", thing, "op", op.ID, "type", op.Type, "err", err)
		return
	}
	if err := sch.Validate(v); err != nil {
		f.node.logger.Warn("fold: marked invalid payload", "log", f.log, "thing", thing, "op", op.ID, "type", op.Type, "err", err)
		return
	}

	switch effect := contract.NormalizeEffect(ts.Effect); effect {
	case contract.EffectNone:
		// The op lives in history; state is not its home.
	case contract.EffectMerge:
		f.mergeState(ctx, thing, op)
	default:
		f.node.logger.Warn("fold: unknown effect treated as none", "log", f.log, "type", op.Type, "effect", effect)
	}
}

// resetState writes a snapshot's state: create-if-absent, else CAS forward.
func (f *fold) resetState(ctx context.Context, thing string, seq uint64, state json.RawMessage) {
	f.casState(ctx, thing, seq, func(*contract.StateValue) (json.RawMessage, bool) {
		return state, true
	})
}

// mergeState applies a merge-effect op onto the thing's current state. An
// op on a subject with no snapshot yet takes no effect: the log is
// malformed for state until one appears (pattern § 5.1).
func (f *fold) mergeState(ctx context.Context, thing string, op contract.Op) {
	f.casState(ctx, thing, op.Seq, func(cur *contract.StateValue) (json.RawMessage, bool) {
		if cur == nil {
			f.node.logger.Warn("fold: op before any snapshot takes no effect", "log", f.log, "thing", thing, "op", op.ID)
			return nil, false
		}
		merged, err := contract.MergePatch(cur.State, op.Payload)
		if err != nil {
			f.node.logger.Warn("fold: merge failed; marked", "log", f.log, "thing", thing, "op", op.ID, "err", err)
			return nil, false
		}
		return merged, true
	})
}

// casState runs the state bucket's optimistic-concurrency loop: whoever
// folds writes, stale writers lose, replays skip, nothing to clean up.
// compute receives the current value (nil when the key is absent) and
// returns the next state and whether to write it.
func (f *fold) casState(ctx context.Context, thing string, seq uint64, compute func(*contract.StateValue) (json.RawMessage, bool)) {
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := f.states.Get(ctx, thing)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			next, write := compute(nil)
			if !write {
				return
			}
			value, merr := json.Marshal(contract.StateValue{Seq: seq, State: next})
			if merr != nil {
				f.node.logger.Warn("fold: encode state", "log", f.log, "thing", thing, "err", merr)
				return
			}
			if _, cerr := f.states.Create(ctx, thing, value); cerr != nil {
				if errors.Is(cerr, jetstream.ErrKeyExists) {
					continue // lost the first-write race; re-read
				}
				f.node.logger.Warn("fold: create state", "log", f.log, "thing", thing, "err", cerr)
			}
			return
		}
		if err != nil {
			f.node.logger.Warn("fold: read state", "log", f.log, "thing", thing, "err", err)
			return
		}
		var cur contract.StateValue
		if err := json.Unmarshal(entry.Value(), &cur); err == nil && cur.Seq >= seq {
			return // the stored state is already at or beyond this op
		}
		next, write := compute(&cur)
		if !write {
			return
		}
		value, merr := json.Marshal(contract.StateValue{Seq: seq, State: next})
		if merr != nil {
			f.node.logger.Warn("fold: encode state", "log", f.log, "thing", thing, "err", merr)
			return
		}
		if _, err := f.states.Update(ctx, thing, value, entry.Revision()); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				continue // revision conflict; re-read and re-judge
			}
			f.node.logger.Warn("fold: update state", "log", f.log, "thing", thing, "err", err)
		}
		return
	}
	f.node.logger.Warn("fold: state write race did not settle", "log", f.log, "thing", thing)
}
