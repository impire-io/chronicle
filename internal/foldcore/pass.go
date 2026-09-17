package foldcore

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/impire-io/chronicle/contract"
)

// Pass is one fold pass's per-op state machine — the fold rules of 0011
// under the type records of 0021/0022, existing once (0023 § 3): resolve
// the thing, judge the op, keep the per-thing frontier and state, and
// hand every derived state to the sink. Lifecycles — consumers, rebuild
// strategies, swaps — stay with the callers; the rules live here. A
// rebuild is a fresh Pass: derived state is rebuilt by replay.
type Pass struct {
	// Resolve classifies a thing tail: the pair walk (ResolveThing) for
	// tenant logs, by-family for the fleet log.
	Resolve func(ctx context.Context, thing string) (contract.Resolution, error)
	// Sink receives a thing's freshly derived state at its seq — the
	// engine's side: a KV-CAS write, an index upsert, a fleet mirror.
	// It fires only when state moved; the frontier advances regardless.
	Sink func(ctx context.Context, thing string, seq uint64, state json.RawMessage)
	// Track, when set, hears every folded op's thing before judgment —
	// the roll-up sweep's active-set feed.
	Track func(thing string)
	// Warn is the pass's logger; the caller's closure carries its label.
	Warn func(msg string, args ...any)

	mu     sync.Mutex
	states map[string]*passState
}

type passState struct {
	// seq is the frontier — the last op seen, whatever it moved; the
	// idempotency guard against redelivery.
	seq uint64
	// stateSeq is the last op that moved state — what Snapshot reports,
	// the fleet's knowledge horizon.
	stateSeq    uint64
	state       json.RawMessage
	sawSnapshot bool
}

// Fold applies one op. Ordered consumers redeliver on gaps, so Fold is
// idempotent: the per-thing frontier skips anything at or below what was
// already folded, and the frontier advances even when the op moves no
// state — it is "what this pass has seen", the fleet's knowledge horizon.
func (p *Pass) Fold(ctx context.Context, thing string, op contract.Op) {
	if p.Track != nil {
		p.Track(thing)
	}
	p.mu.Lock()
	if p.states == nil {
		p.states = map[string]*passState{}
	}
	st, ok := p.states[thing]
	if !ok {
		st = &passState{}
		p.states[thing] = st
	}
	if op.Seq <= st.seq {
		p.mu.Unlock()
		return
	}
	st.seq = op.Seq
	p.mu.Unlock()

	res, err := p.Resolve(ctx, thing)
	if err != nil {
		p.Warn("resolve failed; op takes no effect", "thing", thing, "op", op.ID, "err", err)
		return
	}
	if res.Kind == contract.ResolvedUndeclared {
		p.Warn("marked undeclared aspect", "thing", thing, "op", op.ID, "detail", res.Detail)
		return
	}

	if op.Type == contract.OpTypeSnapshot {
		snap, err := contract.ParseSnapshot(op.Payload)
		if err != nil {
			p.Warn("marked malformed snapshot", "thing", thing, "op", op.ID, "err", err)
			return
		}
		if res.Kind == contract.ResolvedTyped {
			if detail := JudgeSnapshot(res.Record, snap.State); detail != "" {
				p.Warn("marked snapshot state", "thing", thing, "op", op.ID, "detail", detail)
				return
			}
		}
		p.commit(ctx, thing, st, op.Seq, snap.State, true)
		return
	}

	var decision Decision
	var detail string
	if res.Kind == contract.ResolvedTyped {
		decision, detail = JudgeRecord(res.Record, op)
	} else {
		decision, detail = UnknownType, "thing is untyped: "+res.Detail
	}
	switch decision {
	case Merge:
		p.mu.Lock()
		saw, cur := st.sawSnapshot, st.state
		p.mu.Unlock()
		if !saw {
			p.Warn("op before any snapshot takes no effect", "thing", thing, "op", op.ID)
			return
		}
		merged, err := contract.MergePatch(cur, op.Payload)
		if err != nil {
			p.Warn("merge failed; marked", "thing", thing, "op", op.ID, "err", err)
			return
		}
		p.commit(ctx, thing, st, op.Seq, merged, true)
	case None:
		// The op lives in history; state is not its home.
	case UnknownType:
		p.Warn("unknown op type ignored", "thing", thing, "op", op.ID, "type", op.Type, "detail", detail)
	case UnknownEffect:
		p.Warn("unknown effect treated as none", "type", op.Type, "detail", detail)
	case BadTypeRecord:
		p.Warn("type record unusable", "type", op.Type, "detail", detail)
	case Invalid:
		p.Warn("marked invalid payload", "thing", thing, "op", op.ID, "type", op.Type, "detail", detail)
	}
}

// commit stores the moved state and hands it to the sink.
func (p *Pass) commit(ctx context.Context, thing string, st *passState, seq uint64, state json.RawMessage, snap bool) {
	p.mu.Lock()
	st.state = state
	st.stateSeq = seq
	if snap {
		st.sawSnapshot = true
	}
	p.mu.Unlock()
	if p.Sink != nil {
		p.Sink(ctx, thing, seq, state)
	}
}

// Seed pre-loads one thing's state — the checkpoint bootstrap (0023 § 4):
// a state-sourced pass may start from the state index's {seq, state} and
// consume from past it, instead of replaying from sequence 1.
func (p *Pass) Seed(thing string, seq uint64, state json.RawMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.states == nil {
		p.states = map[string]*passState{}
	}
	p.states[thing] = &passState{seq: seq, stateSeq: seq, state: state, sawSnapshot: true}
}

// Snapshot reads one thing's folded state and the seq of the last op
// that moved it — the fleet's knowledge-horizon read.
func (p *Pass) Snapshot(thing string) (json.RawMessage, uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.states[thing]
	if !ok {
		return nil, 0, false
	}
	return st.state, st.stateSeq, true
}

// Things names every thing this pass has seen — the fleet's roster scan.
func (p *Pass) Things() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	things := make([]string, 0, len(p.states))
	for thing := range p.states {
		things = append(things, thing)
	}
	return things
}
