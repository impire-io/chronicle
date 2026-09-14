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

// Config wires one projection.
type Config struct {
	// Log is the log this projection folds.
	Log string
	// Index names the declaration, for log lines.
	Index string
	// Kind labels log lines ("search index", "graph index").
	Kind string
	// NewRun opens a fresh materialization for one fold pass — the boot
	// replay or an effect-change rebuild.
	NewRun func() (Run, error)
	// Logger; nil means slog.Default.
	Logger *slog.Logger
}

// Projection is one running spine.
type Projection struct {
	cfg    Config
	logger *slog.Logger

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
		logger:    logger,
		nc:        nc,
		js:        js,
		meta:      meta,
		stream:    stream,
		rebuildCh: make(chan struct{}, 1),
		runCtx:    runCtx,
		cancel:    cancel,
	}

	if err := p.watchTypes(); err != nil {
		cancel()
		return nil, err
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

// pass is one fold over the log: its own engine run, its own per-thing
// states, its own backlog countdown. Only the consume callback touches
// its fields.
type pass struct {
	run     Run
	states  map[string]*thingState
	pending uint64
	ready   chan struct{}
	p       *Projection
}

type thingState struct {
	seq         uint64
	state       json.RawMessage
	sawSnapshot bool
}

// startRun measures the ops-family backlog, then consumes from sequence
// 1. The consumer is filtered, so the stream head alone cannot say when
// the fold is caught up — the head may be another family's message. When
// the measured backlog reaches zero the pass's run is swapped into
// serving and ready closes; the consumer keeps running as the live tail.
func (p *Projection) startRun(ctx context.Context) (jetstream.ConsumeContext, chan struct{}, Run, error) {
	run, err := p.cfg.NewRun()
	if err != nil {
		return nil, nil, nil, err
	}
	sinfo, err := p.stream.Info(ctx, jetstream.WithSubjectFilter(contract.OpsFilter(p.cfg.Log)))
	if err != nil {
		closeRun(run)
		return nil, nil, nil, fmt.Errorf("stream info: %w", err)
	}
	var pending uint64
	for _, n := range sinfo.State.Subjects {
		pending += n
	}

	ps := &pass{run: run, states: map[string]*thingState{}, pending: pending, ready: make(chan struct{}), p: p}
	if pending == 0 {
		p.serving.set(run)
		close(ps.ready)
	}

	cons, err := p.stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{contract.OpsFilter(p.cfg.Log)},
	})
	if err != nil {
		closeRun(run)
		return nil, nil, nil, fmt.Errorf("ordered consumer: %w", err)
	}
	cc, err := cons.Consume(ps.apply)
	if err != nil {
		closeRun(run)
		return nil, nil, nil, fmt.Errorf("consume: %w", err)
	}
	return cc, ps.ready, run, nil
}

// apply folds one message into the pass. Ordered consumers redeliver on
// gaps, so apply stays idempotent: the per-thing seq skips anything at or
// below what was already folded.
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
	st, ok := ps.states[thing]
	if !ok {
		st = &thingState{}
		ps.states[thing] = st
	}
	if op.Seq <= st.seq {
		return
	}
	st.seq = op.Seq

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if op.Type == contract.OpTypeSnapshot {
		snap, err := contract.ParseSnapshot(op.Payload)
		if err != nil {
			p.logger.Warn(kind+": marked malformed snapshot", "log", p.cfg.Log, "thing", thing, "op", op.ID, "err", err)
			return
		}
		st.state = snap.State
		st.sawSnapshot = true
		ps.run.Upsert(thing, st.state)
		return
	}

	decision, detail := foldcore.Judge(ctx, p.meta, p.cfg.Log, op)
	switch decision {
	case foldcore.Merge:
		if !st.sawSnapshot {
			p.logger.Warn(kind+": op before any snapshot takes no effect", "log", p.cfg.Log, "thing", thing, "op", op.ID)
			return
		}
		merged, err := contract.MergePatch(st.state, op.Payload)
		if err != nil {
			p.logger.Warn(kind+": merge failed; marked", "log", p.cfg.Log, "thing", thing, "op", op.ID, "err", err)
			return
		}
		st.state = merged
		ps.run.Upsert(thing, st.state)
	case foldcore.None:
		// The op lives in history; no index is its home.
	case foldcore.UnknownType:
		p.logger.Warn(kind+": unknown op type ignored", "log", p.cfg.Log, "thing", thing, "op", op.ID, "type", op.Type)
	case foldcore.UnknownEffect:
		p.logger.Warn(kind+": unknown effect treated as none", "log", p.cfg.Log, "type", op.Type, "detail", detail)
	case foldcore.BadTypeRecord:
		p.logger.Warn(kind+": type record unusable", "log", p.cfg.Log, "type", op.Type, "detail", detail)
	case foldcore.Invalid:
		p.logger.Warn(kind+": marked invalid payload", "log", p.cfg.Log, "thing", thing, "op", op.ID, "type", op.Type, "detail", detail)
	}
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

// watchTypes watches the log's type declarations and signals a rebuild
// when a type's effect changes: latest declaration wins (0011 § 3), so
// the index's derivation is suspect exactly as the state bucket's is —
// and suspect derived state is rebuilt by replay. Schema-only revisions
// change no effect and trigger nothing.
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
		effects := map[string]string{}
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
				opType := strings.TrimPrefix(entry.Key(), contract.MetaLogType(p.cfg.Log, ""))
				effect := contract.EffectNone
				if entry.Operation() == jetstream.KeyValuePut {
					var ts contract.TypeSchema
					if err := json.Unmarshal(entry.Value(), &ts); err == nil {
						effect = contract.NormalizeEffect(ts.Effect)
					}
				}
				prev := contract.NormalizeEffect(effects[opType])
				effects[opType] = effect
				if inited && prev != effect {
					p.logger.Info(p.cfg.Kind+": effect changed; re-folding", "log", p.cfg.Log, "index", p.cfg.Index, "type", opType, "effect", effect)
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
