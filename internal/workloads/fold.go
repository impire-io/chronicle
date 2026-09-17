package workloads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/foldcore"
)

// The fleet fold: an ordered consumer over the fleet log's ops family,
// judged by the shared core like any tenant log's (that is the point of
// running scheduling on chronicle's own pattern). It maintains two
// projections of the same order — the instance's in-memory state, whose
// per-thing sequence is the knowledge horizon every custody write stamps,
// and STATE_FLEET for every other reader (control's creds check, the CLI).

func (s *service) startFold(ctx context.Context) error {
	stream, err := s.js.Stream(ctx, contract.StreamName(contract.FleetLog))
	if err != nil {
		return fmt.Errorf("open fleet stream: %w", err)
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{contract.OpsFilter(contract.FleetLog)},
	})
	if err != nil {
		return fmt.Errorf("ordered consumer: %w", err)
	}
	cc, err := cons.Consume(s.apply)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	var once sync.Once
	s.foldStop = func() {
		once.Do(cc.Stop)
		<-cc.Closed()
	}
	return nil
}

// apply folds one op into memory and STATE_FLEET. Ordered consumers
// redeliver on gaps, so apply stays idempotent: memory and the bucket both
// skip anything at or below their stored sequence.
func (s *service) apply(msg jetstream.Msg) {
	md, err := msg.Metadata()
	if err != nil {
		s.logger.Warn("fleet fold: message without metadata", "err", err)
		return
	}
	op := contract.ParseOp(msg.Subject(), md.Sequence.Stream, msg.Headers(), msg.Data())
	thing := contract.ThingFromSubject(contract.FleetLog, op.Subject)
	if thing == op.Subject {
		return // not the ops family
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if op.Type == contract.OpTypeSnapshot {
		snap, err := contract.ParseSnapshot(op.Payload)
		if err != nil {
			s.logger.Warn("fleet fold: marked malformed snapshot", "thing", thing, "op", op.ID, "err", err)
			return
		}
		s.foldInto(thing, op.Seq, func(json.RawMessage) (json.RawMessage, bool) {
			return snap.State, true
		})
		s.writeState(ctx, thing, op.Seq)
		s.kickScan()
		return
	}

	// The fleet resolves the type by thing family, not pair addressing:
	// its vocabulary is chronicle's own code (contract.FleetTypeRecords),
	// and its custody tails carry two id tokens.
	family, _, _ := strings.Cut(thing, ".")
	decision, detail := foldcore.JudgeAs(ctx, s.meta, contract.FleetLog, family, op)
	switch decision {
	case foldcore.Merge:
		s.foldInto(thing, op.Seq, func(cur json.RawMessage) (json.RawMessage, bool) {
			if cur == nil {
				s.logger.Warn("fleet fold: op before any snapshot takes no effect", "thing", thing, "op", op.ID)
				return nil, false
			}
			merged, err := contract.MergePatch(cur, op.Payload)
			if err != nil {
				s.logger.Warn("fleet fold: merge failed; marked", "thing", thing, "op", op.ID, "err", err)
				return nil, false
			}
			return merged, true
		})
		s.writeState(ctx, thing, op.Seq)
		s.kickScan()
	case foldcore.None:
		// History only.
	case foldcore.UnknownType:
		s.logger.Warn("fleet fold: unknown op type ignored", "thing", thing, "op", op.ID, "type", op.Type)
	case foldcore.UnknownEffect, foldcore.BadTypeRecord, foldcore.Invalid:
		s.logger.Warn("fleet fold: op takes no effect", "thing", thing, "op", op.ID, "type", op.Type, "detail", detail)
	}
}

// foldInto advances the in-memory projection. The horizon always advances
// to the op's sequence — even when the op moves no state — because the
// horizon is "what this instance has seen", not "what last changed state".
func (s *service) foldInto(thing string, seq uint64, compute func(json.RawMessage) (json.RawMessage, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.mem[thing]
	if !ok {
		e = &memEntry{}
		s.mem[thing] = e
	}
	if seq <= e.seq {
		return
	}
	if next, write := compute(e.state); write {
		e.state = next
	}
	e.seq = seq
}

// writeState mirrors the memory entry into STATE_FLEET under revision CAS:
// whoever folds writes, stale writers lose, replays skip. Every instance
// folds and every instance writes — the values are deterministic, so the
// race is harmless by construction.
func (s *service) writeState(ctx context.Context, thing string, seq uint64) {
	state, memSeq := s.horizon(thing)
	if memSeq < seq || state == nil {
		return
	}
	value, err := json.Marshal(contract.StateValue{Seq: memSeq, State: state})
	if err != nil {
		s.logger.Warn("fleet fold: encode state", "thing", thing, "err", err)
		return
	}
	for attempt := 0; attempt < 5; attempt++ {
		entry, err := s.states.Get(ctx, thing)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			if _, err := s.states.Create(ctx, thing, value); err != nil {
				if errors.Is(err, jetstream.ErrKeyExists) {
					continue
				}
				s.logger.Warn("fleet fold: create state", "thing", thing, "err", err)
			}
			return
		}
		if err != nil {
			s.logger.Warn("fleet fold: read state", "thing", thing, "err", err)
			return
		}
		var cur contract.StateValue
		if err := json.Unmarshal(entry.Value(), &cur); err == nil && cur.Seq >= memSeq {
			return
		}
		if _, err := s.states.Update(ctx, thing, value, entry.Revision()); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				continue
			}
			s.logger.Warn("fleet fold: update state", "thing", thing, "err", err)
		}
		return
	}
}
