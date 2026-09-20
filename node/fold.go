package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/foldcore"
)

// foldRun is one log's running fold; stop is idempotent and returns only
// when consumption has fully ceased.
type foldRun struct {
	f    *fold
	stop func()
}

// startFold runs one log's fold: an ordered consumer over the ops family
// driving the shared pass (0023 § 3 — the fold rules exist once), its
// sink the KV-CAS write into STATE_<LOG>. Idempotent per log.
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

	f := &fold{node: n, log: log, states: states, active: map[string]struct{}{}}
	f.pass = &foldcore.Pass{
		Resolve: func(ctx context.Context, thing string) (contract.Resolution, error) {
			return foldcore.ResolveThing(ctx, n.meta, log, thing)
		},
		Sink:  f.writeState,
		Track: f.track,
		Warn: func(msg string, args ...any) {
			n.logger.Warn("fold: "+msg, append([]any{"log", log}, args...)...)
		},
	}

	// The fold watermark names the declarations this bucket is derived
	// under (0023 § 4) — the checkpoint indexers may bootstrap from. A
	// failed write only disables the shortcut; the fold serves anyway.
	if err := f.writeWatermark(ctx); err != nil {
		n.logger.Warn("fold: watermark not written; checkpoints disabled until the next fold start", "log", log, "err", err)
	}

	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{contract.OpsFilter(log)},
	})
	if err != nil {
		return fmt.Errorf("ordered consumer: %w", err)
	}
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
// suspect (a declaration changed), so stop the fold, purge the bucket —
// watermark included — and re-fold the whole stream under the current
// declarations with a fresh pass: latest declaration wins (0011 § 3). It
// holds the log's derived-state mutex so no rollup publishes a snapshot
// computed mid-change.
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
	pass   *foldcore.Pass

	mu sync.Mutex
	// active is the timer trigger's feed: every subject this fold saw an
	// op on since the last sweep (04-fleet.md § the node's duties). A
	// restart or rebuild re-folds the stream, so everything re-enters —
	// the next sweep is a full one, by design.
	active map[string]struct{}
}

// track feeds the sweep's active set.
func (f *fold) track(thing string) {
	f.mu.Lock()
	f.active[thing] = struct{}{}
	f.mu.Unlock()
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

// apply folds one message through the shared pass.
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	f.pass.Fold(ctx, thing, op)
}

// writeWatermark records the declarations the bucket derives under.
func (f *fold) writeWatermark(ctx context.Context) error {
	fp, err := foldcore.LogFingerprint(ctx, f.node.meta, f.log)
	if err != nil {
		return err
	}
	value, err := json.Marshal(contract.FoldWatermark{Declarations: fp})
	if err != nil {
		return err
	}
	_, err = f.states.Put(ctx, contract.StateFoldKey, value)
	return err
}

// writeState is the pass's sink: the state bucket's optimistic-
// concurrency write — whoever folds writes, stale writers lose, replays
// skip, nothing to clean up. Values are deterministic under one set of
// declarations, so a racing writer is harmless by construction.
func (f *fold) writeState(ctx context.Context, thing string, seq uint64, state json.RawMessage) {
	value, err := json.Marshal(contract.StateValue{Seq: seq, State: state})
	if err != nil {
		f.node.logger.Warn("fold: encode state", "log", f.log, "thing", thing, "err", err)
		return
	}
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := f.states.Get(ctx, thing)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
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
		if _, err := f.states.Update(ctx, thing, value, entry.Revision()); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				continue // revision conflict; re-read
			}
			f.node.logger.Warn("fold: update state", "log", f.log, "thing", thing, "err", err)
		}
		return
	}
	f.node.logger.Warn("fold: state write race did not settle", "log", f.log, "thing", thing)
}
