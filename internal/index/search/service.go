package search

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/foldcore"
	"github.com/impire-io/chronicle/internal/registry"
	"github.com/impire-io/chronicle/internal/version"
)

// Config names the one index this service materializes.
type Config struct {
	Log   string
	Index string
	// Logger receives the fold's warnings; nil means slog.Default.
	Logger *slog.Logger
}

// Service is one running chronicle-index-search instance.
type Service struct {
	log    string
	index  string
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

	micro micro.Service

	mu      sync.Mutex
	current jetstream.ConsumeContext
}

// Start materializes the index by replaying the log from sequence 1 and
// registers the query endpoint only once the fold has caught up with the
// stream head observed here — until then the subject has no responder,
// the honest signal of an index that is not current yet (05-indexes.md).
// The consumer keeps running as the live tail; a changed effect re-folds
// into a fresh index that is swapped in whole when it catches up.
func Start(ctx context.Context, nc *nats.Conn, cfg Config) (*Service, error) {
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
	s := &Service{
		log:       cfg.Log,
		index:     cfg.Index,
		logger:    logger,
		nc:        nc,
		js:        js,
		meta:      meta,
		stream:    stream,
		rebuildCh: make(chan struct{}, 1),
		runCtx:    runCtx,
		cancel:    cancel,
	}

	if err := s.watchTypes(); err != nil {
		cancel()
		return nil, err
	}

	cc, ready, err := s.startRun(ctx)
	if err != nil {
		cancel()
		s.wg.Wait()
		return nil, err
	}
	s.current = cc
	select {
	case <-ready:
	case <-ctx.Done():
		cancel()
		cc.Stop()
		s.wg.Wait()
		return nil, fmt.Errorf("catching up with the log: %w", ctx.Err())
	}

	s.wg.Add(1)
	go s.manage()

	m, err := micro.AddService(nc, micro.Config{
		Name:        "chronicle-index-search",
		Version:     version.Version,
		Description: "chronicle search index: a full-text projection of thing state",
		Metadata:    map[string]string{"log": cfg.Log, "index": cfg.Index},
	})
	if err != nil {
		s.teardown()
		return nil, fmt.Errorf("register service: %w", err)
	}
	if err := m.AddEndpoint("query", micro.HandlerFunc(s.handleQuery),
		micro.WithEndpointSubject(client.IndexQuerySubject(cfg.Log, cfg.Index))); err != nil {
		_ = m.Stop()
		s.teardown()
		return nil, fmt.Errorf("add query endpoint: %w", err)
	}
	// Flush so the endpoint subscription has reached the server: once
	// Start returns, a request from any connection must find a responder.
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = m.Stop()
		s.teardown()
		return nil, fmt.Errorf("flush endpoint subscription: %w", err)
	}
	s.micro = m
	return s, nil
}

// Stop takes the endpoint off the wire, then stops the fold, the type
// watcher, and the rebuild manager.
func (s *Service) Stop() {
	if s.micro != nil {
		_ = s.micro.Stop()
	}
	s.teardown()
}

func (s *Service) teardown() {
	// The manager may be mid-swap: let it exit before touching current,
	// so the consumer it stored is the one stopped here.
	s.cancel()
	s.wg.Wait()
	s.mu.Lock()
	cc := s.current
	s.current = nil
	s.mu.Unlock()
	if cc != nil {
		cc.Stop()
	}
}

// run is one fold pass over the log: its own index, its own per-thing
// states, its own backlog countdown. The first run is the boot replay;
// later ones are effect-change rebuilds. Only the consume callback
// touches its fields.
type run struct {
	idx     bleve.Index
	states  map[string]*thingState
	pending uint64
	ready   chan struct{}
	svc     *Service
}

type thingState struct {
	seq         uint64
	state       json.RawMessage
	sawSnapshot bool
}

// startRun measures the ops-family backlog, then consumes from sequence 1.
// The consumer is filtered, so the stream head alone cannot say when the
// fold is caught up — the head may be another family's message. When the
// measured backlog reaches zero the run's index is swapped into serving
// and ready closes; the consumer keeps running as the live tail.
func (s *Service) startRun(ctx context.Context) (jetstream.ConsumeContext, chan struct{}, error) {
	idx, err := newBleveIndex()
	if err != nil {
		return nil, nil, err
	}
	sinfo, err := s.stream.Info(ctx, jetstream.WithSubjectFilter(contract.OpsFilter(s.log)))
	if err != nil {
		return nil, nil, fmt.Errorf("stream info: %w", err)
	}
	var pending uint64
	for _, n := range sinfo.State.Subjects {
		pending += n
	}

	r := &run{idx: idx, states: map[string]*thingState{}, pending: pending, ready: make(chan struct{}), svc: s}
	if pending == 0 {
		s.serving.set(idx)
		close(r.ready)
	}

	cons, err := s.stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{contract.OpsFilter(s.log)},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("ordered consumer: %w", err)
	}
	cc, err := cons.Consume(r.apply)
	if err != nil {
		return nil, nil, fmt.Errorf("consume: %w", err)
	}
	return cc, r.ready, nil
}

// apply folds one message into the run. Ordered consumers redeliver on
// gaps, so apply stays idempotent: the per-thing seq skips anything at or
// below what was already folded.
func (r *run) apply(msg jetstream.Msg) {
	defer r.countdown()

	s := r.svc
	md, err := msg.Metadata()
	if err != nil {
		s.logger.Warn("search index: message without metadata", "log", s.log, "err", err)
		return
	}
	op := contract.ParseOp(msg.Subject(), md.Sequence.Stream, msg.Headers(), msg.Data())
	thing := contract.ThingFromSubject(s.log, op.Subject)
	if thing == op.Subject {
		// Not the ops family; the filter should not deliver this, but a
		// projection stays tolerant.
		return
	}
	st, ok := r.states[thing]
	if !ok {
		st = &thingState{}
		r.states[thing] = st
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
			s.logger.Warn("search index: marked malformed snapshot", "log", s.log, "thing", thing, "op", op.ID, "err", err)
			return
		}
		st.state = snap.State
		st.sawSnapshot = true
		r.upsert(thing, st.state)
		return
	}

	decision, detail := foldcore.Judge(ctx, s.meta, s.log, op)
	switch decision {
	case foldcore.Merge:
		if !st.sawSnapshot {
			s.logger.Warn("search index: op before any snapshot takes no effect", "log", s.log, "thing", thing, "op", op.ID)
			return
		}
		merged, err := contract.MergePatch(st.state, op.Payload)
		if err != nil {
			s.logger.Warn("search index: merge failed; marked", "log", s.log, "thing", thing, "op", op.ID, "err", err)
			return
		}
		st.state = merged
		r.upsert(thing, st.state)
	case foldcore.None:
		// The op lives in history; no index is its home.
	case foldcore.UnknownType:
		s.logger.Warn("search index: unknown op type ignored", "log", s.log, "thing", thing, "op", op.ID, "type", op.Type)
	case foldcore.UnknownEffect:
		s.logger.Warn("search index: unknown effect treated as none", "log", s.log, "type", op.Type, "detail", detail)
	case foldcore.BadTypeRecord:
		s.logger.Warn("search index: type record unusable", "log", s.log, "type", op.Type, "detail", detail)
	case foldcore.Invalid:
		s.logger.Warn("search index: marked invalid payload", "log", s.log, "thing", thing, "op", op.ID, "type", op.Type, "detail", detail)
	}
}

// countdown counts the measured backlog off; at zero the run's index
// becomes the served one. Only the consume goroutine touches pending.
func (r *run) countdown() {
	if r.pending == 0 {
		return
	}
	r.pending--
	if r.pending == 0 {
		r.svc.serving.set(r.idx)
		close(r.ready)
	}
}

func (r *run) upsert(thing string, state json.RawMessage) {
	doc, err := docFor(state)
	if err != nil {
		r.svc.logger.Warn("search index: state not indexable", "log", r.svc.log, "thing", thing, "err", err)
		return
	}
	if err := r.idx.Index(thing, doc); err != nil {
		r.svc.logger.Warn("search index: index thing", "log", r.svc.log, "thing", thing, "err", err)
	}
}

// watchTypes watches the log's type declarations and signals a rebuild
// when a type's effect changes: latest declaration wins (0011 § 3), so
// the index's derivation is suspect exactly as the state bucket's is —
// and suspect derived state is rebuilt by replay. Schema-only revisions
// change no effect and trigger nothing.
func (s *Service) watchTypes() error {
	prefix := contract.MetaLogType(s.log, "") + ">"
	w, err := s.meta.Watch(s.runCtx, prefix)
	if err != nil {
		return fmt.Errorf("watch type declarations: %w", err)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { _ = w.Stop() }()
		effects := map[string]string{}
		inited := false
		for {
			select {
			case <-s.runCtx.Done():
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
				opType := strings.TrimPrefix(entry.Key(), contract.MetaLogType(s.log, ""))
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
					s.logger.Info("search index: effect changed; re-folding", "log", s.log, "index", s.index, "type", opType, "effect", effect)
					select {
					case s.rebuildCh <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	return nil
}

// manage serves rebuild signals: fold the whole log again into a fresh
// index, and only when the new fold has caught up swap it in and retire
// the old consumer — queries never see a half-rebuilt index.
func (s *Service) manage() {
	defer s.wg.Done()
	for {
		select {
		case <-s.runCtx.Done():
			return
		case <-s.rebuildCh:
			ctx, cancel := context.WithTimeout(s.runCtx, time.Minute)
			cc, ready, err := s.startRun(ctx)
			cancel()
			if err != nil {
				s.logger.Warn("search index: rebuild failed; serving the previous fold", "log", s.log, "index", s.index, "err", err)
				continue
			}
			select {
			case <-ready:
				s.mu.Lock()
				old := s.current
				s.current = cc
				s.mu.Unlock()
				if old != nil {
					old.Stop()
				}
			case <-s.runCtx.Done():
				cc.Stop()
				return
			}
		}
	}
}

// handleQuery answers CHRON.API.INDEX.QUERY.<log>.<index>. Any registry
// role may query — search is a read, and the member baseline already lets
// a member replay the whole log.
func (s *Service) handleQuery(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var r client.IndexQueryRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if err := registry.RequireRole(ctx, s.meta, r.Principal, contract.RoleAdmin, contract.RoleWriter, contract.RoleReader); err != nil {
		_ = req.Error("forbidden", err.Error(), nil)
		return
	}
	reply, err := s.serving.query(r)
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	data, err := json.Marshal(reply)
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(data)
}
