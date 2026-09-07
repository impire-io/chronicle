package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// startFold runs one log's fold: an ordered consumer over the ops family,
// one fold state per subject, never letting one subject's op touch
// another's. Idempotent per log.
func (n *node) startFold(ctx context.Context, log string) error {
	n.mu.Lock()
	if n.folds[log] {
		n.mu.Unlock()
		return nil
	}
	n.folds[log] = true
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

	f := &fold{node: n, log: log, states: states}
	cc, err := cons.Consume(f.apply)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		<-n.foldCtx.Done()
		cc.Stop()
	}()
	return nil
}

type fold struct {
	node   *node
	log    string
	states jetstream.KeyValue
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if op.Type == contract.OpTypeSnapshot {
		snap, err := contract.ParseSnapshot(op.Payload)
		if err != nil {
			f.node.logger.Warn("fold: marked malformed snapshot", "log", f.log, "thing", thing, "op", op.ID, "err", err)
			return
		}
		f.writeState(ctx, thing, op.Seq, snap.State)
		return
	}

	// Non-snapshot ops: chronicle applies no customer op semantics — the
	// state bucket carries the pattern's floor (latest snapshot at its
	// seq; tracker item 27 holds the default-fold question). The fold
	// still reads the vocabulary: unknown types are warned about, and a
	// schema-invalid payload of a known type is marked, never dropped.
	f.validate(ctx, op, thing)
}

// writeState CAS-writes {seq, state}: whoever folds writes, stale writers
// lose, nothing to clean up.
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
				continue // revision conflict; re-read and re-judge
			}
			f.node.logger.Warn("fold: update state", "log", f.log, "thing", thing, "err", err)
		}
		return
	}
	f.node.logger.Warn("fold: state write race did not settle", "log", f.log, "thing", thing)
}

// validate is the projection's tolerance, stated by the wire contract:
// unknown op types are ignored with a warning; a malformed payload of a
// known type is marked, never dropped. Junk can land in the log; the log
// stays the record, warts included.
func (f *fold) validate(ctx context.Context, op contract.Op, thing string) {
	entry, err := f.node.meta.Get(ctx, contract.MetaLogType(f.log, op.Type))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		f.node.logger.Warn("fold: unknown op type ignored", "log", f.log, "thing", thing, "op", op.ID, "type", op.Type)
		return
	}
	if err != nil {
		f.node.logger.Warn("fold: read schema", "log", f.log, "type", op.Type, "err", err)
		return
	}
	var ts contract.TypeSchema
	if err := json.Unmarshal(entry.Value(), &ts); err != nil {
		f.node.logger.Warn("fold: decode schema record", "log", f.log, "type", op.Type, "err", err)
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
	}
}
