// Package projection is the state-materialization spine every index kind
// shares (05-indexes.md): replay the log from sequence 1 through the
// shared judge, keep the live tail, watch the log's type declarations and
// re-fold when an effect changes — latest declaration wins, suspect
// derived state is rebuilt by replay — and swap a rebuilt materialization
// in whole, so queries never see a half-built one. The engine behind it
// is two methods; search was the first consumer, graph the second, and a
// third copy of this spine was the drift hazard foldcore's extraction
// named, one layer up.
package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/foldcore"
)

// Run is one fold pass's materialization target. Upsert receives a
// thing's freshly folded state — latest state wins, and the engine owns
// whatever it derives from it (documents, edges, vectors). Upsert logs
// its own troubles; the spine has already decided the state is real.
type Run interface {
	Upsert(thing string, state json.RawMessage)
}

// OpRun is the target of an ops-sourced pass (0020): each op is its own
// document, keyed by thing and stream sequence. Ops are immutable, so a
// document handed over is handed over once. An ops-sourced engine
// implements this beside Run.
type OpRun interface {
	UpsertOp(thing string, seq uint64, payload json.RawMessage)
}

// DocID keys one op's document: the thing tail and the op's stream seq,
// NUL-separated — NUL cannot appear in a subject token, so the split is
// unambiguous against any thing name.
func DocID(thing string, seq uint64) string {
	return thing + "\x00" + strconv.FormatUint(seq, 10)
}

// DocThing names the thing a document ID belongs to — the ID itself when
// it carries no seq (a state-sourced document).
func DocThing(id string) string {
	if i := strings.IndexByte(id, 0); i >= 0 {
		return id[:i]
	}
	return id
}

// Config wires one projection.
type Config struct {
	// Log is the log this projection folds.
	Log string
	// Index names the declaration, for log lines.
	Index string
	// Kind labels log lines ("search index", "graph index").
	Kind string
	// Source is the declaration's source (0020): state (the default) or
	// ops. An ops-sourced pass hands every op to the engine as its own
	// document — no fold, no effects, no snapshot gate — and never
	// rebuilds on an effect change, because effects don't shape op
	// documents.
	Source string
	// Types narrows which op types an ops-sourced pass reads; empty
	// means every op.
	Types []string
	// NewRun opens a fresh materialization for one fold pass — the boot
	// replay or an effect-change rebuild. For an ops-sourced projection
	// the run must also implement OpRun.
	NewRun func() (Run, error)
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// Projection is one running spine.
type Projection struct {
	cfg       Config
	opsSource bool
	types     map[string]struct{}
	logger    *slog.Logger

	nc     *nats.Conn
	js     jetstream.JetStream
	meta   jetstream.KeyValue
	stream jetstream.Stream

	serving   serving
	rebuildCh chan struct{}

	runCtx context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu         sync.Mutex
	current    jetstream.ConsumeContext
	currentRun Run
}

// Start folds the log and returns only once the first pass has caught up
// with the stream head observed here — the caller registers its query
// endpoint after, so a responder implies a current index.
func Start(ctx context.Context, nc *nats.Conn, cfg Config) (*Projection, error) {
	if cfg.NewRun == nil {
		return nil, fmt.Errorf("projection needs an engine")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return nil, fmt.Errorf("open META: %w", err)
	}
	stream, err := js.Stream(ctx, contract.StreamName(cfg.Log))
	if err != nil {
		return nil, fmt.Errorf("open stream for log %q: %w", cfg.Log, err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	runCtx, cancel := context.WithCancel(context.Background())
	p := &Projection{
		cfg:       cfg,
		opsSource: contract.NormalizeSource(cfg.Source) == contract.SourceOps,
		logger:    logger,
		nc:        nc,
		js:        js,
		meta:      meta,
		stream:    stream,
		rebuildCh: make(chan struct{}, 1),
		runCtx:    runCtx,
		cancel:    cancel,
	}
	if len(cfg.Types) > 0 {
		p.types = make(map[string]struct{}, len(cfg.Types))
		for _, t := range cfg.Types {
			p.types[t] = struct{}{}
		}
	}

	// An ops-sourced pass never folds, so effect changes cannot make it
	// suspect — the type watcher and its rebuilds belong to the state
	// source alone (0020).
	if !p.opsSource {
		if err := p.watchTypes(); err != nil {
			cancel()
			return nil, err
		}
	}

	cc, ready, run, err := p.startRun(ctx)
	if err != nil {
		cancel()
		p.wg.Wait()
		return nil, err
	}
	p.current = cc
	p.currentRun = run
	select {
	case <-ready:
	case <-ctx.Done():
		cancel()
		cc.Stop()
		closeRun(run)
		p.wg.Wait()
		return nil, fmt.Errorf("catching up with the log: %w", ctx.Err())
	}

	p.wg.Add(1)
	go p.manage()
	return p, nil
}

// Serving is the materialization queries read — replaced whole when a
// rebuild catches up, never nil once Start has returned.
func (p *Projection) Serving() Run { return p.serving.get() }

// Meta exposes the log's META bucket for the consumer's own reads —
// role checks, its declaration.
func (p *Projection) Meta() jetstream.KeyValue { return p.meta }

// Stop stops the fold, the type watcher, and the rebuild manager. The
// caller takes its endpoint off the wire first.
func (p *Projection) Stop() {
	p.cancel()
	p.wg.Wait()
	p.mu.Lock()
	cc := p.current
	run := p.currentRun
	p.current = nil
	p.currentRun = nil
	p.mu.Unlock()
	if cc != nil {
		cc.Stop()
	}
	closeRun(run)
}

// closeRun releases an engine run with a lifecycle — a retired run's
// background work (an embed worker) must not outlive its retirement.
func closeRun(r Run) {
	if c, ok := r.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// serving is the swap point between queries and the fold.
type serving struct {
	mu sync.RWMutex
	r  Run
}

func (v *serving) set(r Run) {
	v.mu.Lock()
	v.r = r
	v.mu.Unlock()
}

func (v *serving) get() Run {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.r
}

// pass is one fold over the log: its own engine run, its own backlog
// countdown. A state-sourced pass folds through the shared foldcore.Pass
// (0023 § 3 — the fold rules exist once) with the engine as its sink; an
// ops-sourced pass carries the engine's OpRun face, never folds, and
// keeps only its per-thing seq guard.
type pass struct {
	run     Run
	opRun   OpRun
	fold    *foldcore.Pass
	states  map[string]*thingState
	pending uint64
	ready   chan struct{}
	p       *Projection
}

type thingState struct {
	seq uint64
}

// startRun measures the ops-family backlog, then consumes — from
// sequence 1, or from a state checkpoint when the fold watermark matches
// the current declarations (0023 § 4). The consumer is filtered, so the
// stream head alone cannot say when the fold is caught up — the head may
// be another family's message. When the measured backlog reaches zero
// the pass's run is swapped into serving and ready closes; the consumer
// keeps running as the live tail.
func (p *Projection) startRun(ctx context.Context) (jetstream.ConsumeContext, chan struct{}, Run, error) {
	run, err := p.cfg.NewRun()
	if err != nil {
		return nil, nil, nil, err
	}
	var opRun OpRun
	if p.opsSource {
		var ok bool
		if opRun, ok = run.(OpRun); !ok {
			closeRun(run)
			return nil, nil, nil, fmt.Errorf("the %s engine cannot source from ops", p.cfg.Kind)
		}
	}

	ps := &pass{run: run, opRun: opRun, states: map[string]*thingState{}, ready: make(chan struct{}), p: p}
	consCfg := jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{contract.OpsFilter(p.cfg.Log)},
	}
	if !p.opsSource {
		ps.fold = &foldcore.Pass{
			Resolve: func(ctx context.Context, thing string) (contract.Resolution, error) {
				return foldcore.ResolveThing(ctx, p.meta, p.cfg.Log, thing)
			},
			Sink: func(_ context.Context, thing string, _ uint64, state json.RawMessage) {
				run.Upsert(thing, state)
			},
			Warn: func(msg string, args ...any) {
				p.logger.Warn(p.cfg.Kind+": "+msg, append([]any{"log", p.cfg.Log}, args...)...)
			},
		}
		if from, seeded := p.seedFromCheckpoint(ctx, ps.fold, run); seeded > 0 {
			consCfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
			consCfg.OptStartSeq = from
			p.logger.Info(p.cfg.Kind+": state checkpoint seeded", "log", p.cfg.Log, "index", p.cfg.Index, "things", seeded, "from", from)
		}
	}

	cons, err := p.stream.OrderedConsumer(ctx, consCfg)
	if err != nil {
		closeRun(run)
		return nil, nil, nil, fmt.Errorf("ordered consumer: %w", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		closeRun(run)
		return nil, nil, nil, fmt.Errorf("consumer info: %w", err)
	}
	ps.pending = info.NumPending
	if ps.pending == 0 {
		p.serving.set(run)
		close(ps.ready)
	}
	cc, err := cons.Consume(ps.apply)
	if err != nil {
		closeRun(run)
		return nil, nil, nil, fmt.Errorf("consume: %w", err)
	}
	return cc, ps.ready, run, nil
}

// seedFromCheckpoint loads the state index's {seq, state} values into a
// fresh fold when the bucket's watermark names the current declarations
// (0023 § 4): the pass then consumes from just past the oldest seeded
// seq instead of replaying from sequence 1 — the per-thing guards skip
// what the seeds already cover. Anything less than a clean, matching
// read voids the shortcut and the pass replays whole: the suspicion rule.
func (p *Projection) seedFromCheckpoint(ctx context.Context, fold *foldcore.Pass, run Run) (uint64, int) {
	states, err := p.js.KeyValue(ctx, contract.StateBucket(p.cfg.Log))
	if err != nil {
		return 0, 0
	}
	entry, err := states.Get(ctx, contract.StateFoldKey)
	if err != nil {
		return 0, 0
	}
	var wm contract.FoldWatermark
	if json.Unmarshal(entry.Value(), &wm) != nil {
		return 0, 0
	}
	current, err := foldcore.LogFingerprint(ctx, p.meta, p.cfg.Log)
	if err != nil || wm.Declarations != current {
		return 0, 0
	}
	keys, err := states.Keys(ctx)
	if err != nil {
		return 0, 0
	}
	var minSeq uint64
	seeded := 0
	for _, k := range keys {
		if k == contract.StateFoldKey {
			continue
		}
		e, err := states.Get(ctx, k)
		if err != nil {
			return 0, 0
		}
		var sv contract.StateValue
		if json.Unmarshal(e.Value(), &sv) != nil {
			return 0, 0
		}
		fold.Seed(k, sv.Seq, sv.State)
		run.Upsert(k, sv.State)
		if minSeq == 0 || sv.Seq < minSeq {
			minSeq = sv.Seq
		}
		seeded++
	}
	if seeded == 0 {
		return 0, 0
	}
	return minSeq + 1, seeded
}

// apply folds one message into the pass. Ordered consumers redeliver on
// gaps, so apply stays idempotent: the per-thing seq — the shared pass's
// for the state source, this pass's own for the ops source — skips
// anything at or below what was already folded.
func (ps *pass) apply(msg jetstream.Msg) {
	defer ps.countdown()

	p := ps.p
	kind := p.cfg.Kind
	md, err := msg.Metadata()
	if err != nil {
		p.logger.Warn(kind+": message without metadata", "log", p.cfg.Log, "err", err)
		return
	}
	op := contract.ParseOp(msg.Subject(), md.Sequence.Stream, msg.Headers(), msg.Data())
	thing := contract.ThingFromSubject(p.cfg.Log, op.Subject)
	if thing == op.Subject {
		// Not the ops family; the filter should not deliver this, but a
		// projection stays tolerant.
		return
	}

	if ps.opRun != nil {
		// History as it is (0020): every op is its own document — no
		// judge, no snapshot gate, no effects. Schemas and effects shape
		// state, never history's visibility.
		st, ok := ps.states[thing]
		if !ok {
			st = &thingState{}
			ps.states[thing] = st
		}
		if op.Seq <= st.seq {
			return
		}
		st.seq = op.Seq
		if p.types != nil {
			if _, selected := p.types[op.Type]; !selected {
				return
			}
		}
		ps.opRun.UpsertOp(thing, op.Seq, op.Payload)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ps.fold.Fold(ctx, thing, op)
}

// countdown counts the measured backlog off; at zero the pass's run
// becomes the served one. Only the consume goroutine touches pending.
func (ps *pass) countdown() {
	if ps.pending == 0 {
		return
	}
	ps.pending--
	if ps.pending == 0 {
		ps.p.serving.set(ps.run)
		close(ps.ready)
	}
}

// watchTypes watches the log's type records and signals a rebuild when a
// record's fold fingerprint changes — history, aspects, or an operation's
// effect (0021 § 5): latest declaration wins (0011 § 3), so the index's
// derivation is suspect exactly as the state bucket's is — and suspect
// derived state is rebuilt by replay. Schema-only revisions change no
// fingerprint and trigger nothing.
func (p *Projection) watchTypes() error {
	prefix := contract.MetaLogType(p.cfg.Log, "") + ">"
	w, err := p.meta.Watch(p.runCtx, prefix)
	if err != nil {
		return fmt.Errorf("watch type declarations: %w", err)
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { _ = w.Stop() }()
		baseline := foldcore.FoldFingerprint(nil)
		prints := map[string]string{}
		inited := false
		for {
			select {
			case <-p.runCtx.Done():
				return
			case entry, ok := <-w.Updates():
				if !ok {
					return
				}
				if entry == nil {
					// The initial replay of current declarations is done;
					// anything after this is a live change.
					inited = true
					continue
				}
				typeName := strings.TrimPrefix(entry.Key(), contract.MetaLogType(p.cfg.Log, ""))
				fp := baseline
				if entry.Operation() == jetstream.KeyValuePut {
					var rec contract.TypeRecord
					if err := json.Unmarshal(entry.Value(), &rec); err == nil {
						fp = foldcore.FoldFingerprint(&rec)
					}
				}
				prev, ok := prints[typeName]
				if !ok {
					prev = baseline
				}
				prints[typeName] = fp
				if inited && prev != fp {
					p.logger.Info(p.cfg.Kind+": declaration changed; re-folding", "log", p.cfg.Log, "index", p.cfg.Index, "type", typeName)
					select {
					case p.rebuildCh <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	return nil
}

// manage serves rebuild signals: fold the whole log again into a fresh
// run, and only when the new fold has caught up swap it in and retire
// the old consumer — queries never see a half-rebuilt index.
func (p *Projection) manage() {
	defer p.wg.Done()
	for {
		select {
		case <-p.runCtx.Done():
			return
		case <-p.rebuildCh:
			ctx, cancel := context.WithTimeout(p.runCtx, time.Minute)
			cc, ready, run, err := p.startRun(ctx)
			cancel()
			if err != nil {
				p.logger.Warn(p.cfg.Kind+": rebuild failed; serving the previous fold", "log", p.cfg.Log, "index", p.cfg.Index, "err", err)
				continue
			}
			select {
			case <-ready:
				p.mu.Lock()
				old := p.current
				oldRun := p.currentRun
				p.current = cc
				p.currentRun = run
				p.mu.Unlock()
				if old != nil {
					old.Stop()
				}
				closeRun(oldRun)
			case <-p.runCtx.Done():
				cc.Stop()
				closeRun(run)
				return
			}
		}
	}
}
