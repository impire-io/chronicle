package workloads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
)

// The fleet fold: an ordered consumer over the fleet log's ops family,
// driving the shared pass (0023 § 3) like any tenant log's — that is the
// point of running scheduling on chronicle's own pattern. The pass keeps
// the instance's in-memory state, whose per-thing sequence is the
// knowledge horizon every custody write stamps, and the sink mirrors
// every move into STATE_FLEET for every other reader (control's creds
// check, the CLI).

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

// apply folds one op through the shared pass. Ordered consumers
// redeliver on gaps; the pass's per-thing frontier keeps apply
// idempotent, and the sink's CAS keeps the mirror so.
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
	s.pass.Fold(ctx, thing, op)
}

// writeState mirrors a state move into STATE_FLEET under revision CAS:
// whoever folds writes, stale writers lose, replays skip. Every instance
// folds and every instance writes — the values are deterministic, so the
// race is harmless by construction.
func (s *service) writeState(ctx context.Context, thing string, seq uint64, state json.RawMessage) {
	value, err := json.Marshal(contract.StateValue{Seq: seq, State: state})
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
		if err := json.Unmarshal(entry.Value(), &cur); err == nil && cur.Seq >= seq {
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
