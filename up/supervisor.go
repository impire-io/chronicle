package up

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/index/graph"
	"github.com/impire-io/chronicle/index/search"
	"github.com/impire-io/chronicle/index/semantic"
)

// supervisor places the declared indexers in this process. It listens
// where the node reports its index slice — the same subject the managed
// service's bridge carries, answered here directly — so a declaration
// starts its kind, a deletion stops it, and the node's boot re-derivation
// brings every declared index back on the next `up`. A declaration is
// still the truth (05-indexes.md): a kind this process cannot serve is
// left declared and honestly unserved, warning said.
type supervisor struct {
	nc        *nats.Conn
	embedding *semantic.ProviderConfig
	logger    *slog.Logger
	sub       *nats.Subscription

	mu      sync.Mutex
	running map[string]func()
}

func startSupervisor(nc *nats.Conn, embedding *semantic.ProviderConfig, logger *slog.Logger) (*supervisor, error) {
	s := &supervisor{nc: nc, embedding: embedding, logger: logger, running: map[string]func(){}}
	sub, err := nc.Subscribe(contract.FleetBridgeLocalSubject, s.handle)
	if err != nil {
		return nil, fmt.Errorf("subscribe to index reports: %w", err)
	}
	s.sub = sub
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("flush report subscription: %w", err)
	}
	return s, nil
}

func (s *supervisor) stop() {
	if s.sub != nil {
		_ = s.sub.Unsubscribe()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, stop := range s.running {
		stop()
		delete(s.running, key)
	}
}

// handle answers one report the way the node expects: the ack on
// success, the micro error headers on refusal.
func (s *supervisor) handle(m *nats.Msg) {
	var r contract.FleetIndexReport
	if err := json.Unmarshal(m.Data, &r); err != nil {
		s.refuse(m, "bad-request", err.Error())
		return
	}
	var err error
	switch r.Action {
	case contract.IndexReportDeclared:
		err = s.start(r.Log, r.Index, r.Kind)
	case contract.IndexReportDeleted:
		s.stopIndex(r.Log, r.Index)
	default:
		s.refuse(m, "bad-action", fmt.Sprintf("report action %q is not declared or deleted", r.Action))
		return
	}
	if err != nil {
		s.refuse(m, "unserved", err.Error())
		return
	}
	ack, _ := json.Marshal(contract.FleetIndexReportAck{Recorded: true})
	_ = m.Respond(ack)
}

func (s *supervisor) refuse(m *nats.Msg, code, desc string) {
	reply := nats.NewMsg(m.Reply)
	reply.Header.Set(micro.ErrorCodeHeader, code)
	reply.Header.Set(micro.ErrorHeader, desc)
	_ = m.RespondMsg(reply)
}

func key(log, index string) string { return log + "/" + index }

// start places one declared index in-process; a second report of the
// same index is the idempotent case.
func (s *supervisor) start(log, index, kind string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.running[key(log, index)]; ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var stop func()
	switch kind {
	case contract.IndexKindSearch:
		svc, err := search.Start(ctx, s.nc, search.Config{Log: log, Index: index, Logger: s.logger})
		if err != nil {
			return fmt.Errorf("start search index %s on %s: %w", index, log, err)
		}
		stop = svc.Stop
	case contract.IndexKindGraph:
		svc, err := graph.Start(ctx, s.nc, graph.Config{Log: log, Index: index, Logger: s.logger})
		if err != nil {
			return fmt.Errorf("start graph index %s on %s: %w", index, log, err)
		}
		stop = svc.Stop
	case contract.IndexKindSemantic:
		if !s.embedding.Configured() {
			s.logger.Warn("semantic index declared but this quick start has no embedding provider; it stays declared and unserved (chronicle up --embedding-url --embedding-model)", "log", log, "index", index)
			return nil
		}
		svc, err := semantic.Start(ctx, s.nc, semantic.Config{Log: log, Index: index, Provider: *s.embedding, Logger: s.logger})
		if err != nil {
			return fmt.Errorf("start semantic index %s on %s: %w", index, log, err)
		}
		stop = svc.Stop
	default:
		s.logger.Warn("index declared with a kind this quick start does not serve; left declared", "log", log, "index", index, "kind", kind)
		return nil
	}
	s.running[key(log, index)] = stop
	s.logger.Info("index placed", "log", log, "index", index, "kind", kind)
	return nil
}

func (s *supervisor) stopIndex(log, index string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stop, ok := s.running[key(log, index)]; ok {
		stop()
		delete(s.running, key(log, index))
		s.logger.Info("index retired", "log", log, "index", index)
	}
}

func jetstreamOf(nc *nats.Conn) (jetstream.JetStream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	return js, nil
}
