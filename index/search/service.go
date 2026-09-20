package search

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/index/projection"
	"github.com/impire-io/chronicle/internal/version"
	"github.com/impire-io/chronicle/registry"
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
	proj  *projection.Projection
	micro micro.Service
}

// Start reads the index's declaration, materializes it over the shared
// projection spine — replay from 1, live tail, effect-change rebuilds for
// the state source — and registers the query endpoint only once the fold
// has caught up: a responder implies a current index (05-indexes.md).
func Start(ctx context.Context, nc *nats.Conn, cfg Config) (*Service, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return nil, fmt.Errorf("open META: %w", err)
	}
	entry, err := meta.Get(ctx, contract.MetaIndex(cfg.Log, cfg.Index))
	if err != nil {
		return nil, fmt.Errorf("read declaration for %s/%s: %w", cfg.Log, cfg.Index, err)
	}
	var decl contract.IndexDeclaration
	if err := json.Unmarshal(entry.Value(), &decl); err != nil {
		return nil, fmt.Errorf("decode declaration: %w", err)
	}
	if decl.Kind != contract.IndexKindSearch {
		return nil, fmt.Errorf("declaration %s/%s is kind %q, not search", cfg.Log, cfg.Index, decl.Kind)
	}
	scfg, err := contract.ParseSearchConfig(decl.Config)
	if err != nil {
		return nil, err
	}
	ops := contract.NormalizeSource(scfg.Source) == contract.SourceOps

	proj, err := projection.Start(ctx, nc, projection.Config{
		Log:    cfg.Log,
		Index:  cfg.Index,
		Kind:   "search index",
		Source: scfg.Source,
		Types:  scfg.Types,
		Logger: logger,
		NewRun: func() (projection.Run, error) { return newSearchRun(cfg.Log, ops, logger) },
	})
	if err != nil {
		return nil, err
	}
	s := &Service{proj: proj}

	m, err := micro.AddService(nc, micro.Config{
		Name:        "chronicle-index-search",
		Version:     version.Version,
		Description: "chronicle search index: a full-text projection of thing state",
		Metadata:    map[string]string{"log": cfg.Log, "index": cfg.Index},
	})
	if err != nil {
		proj.Stop()
		return nil, fmt.Errorf("register service: %w", err)
	}
	if err := m.AddEndpoint("query", micro.HandlerFunc(s.handleQuery),
		micro.WithEndpointSubject(client.IndexQuerySubject(cfg.Log, cfg.Index))); err != nil {
		_ = m.Stop()
		proj.Stop()
		return nil, fmt.Errorf("add query endpoint: %w", err)
	}
	// Flush so the endpoint subscription has reached the server: once
	// Start returns, a request from any connection must find a responder.
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = m.Stop()
		proj.Stop()
		return nil, fmt.Errorf("flush endpoint subscription: %w", err)
	}
	s.micro = m
	return s, nil
}

// Stop takes the endpoint off the wire, then stops the projection.
func (s *Service) Stop() {
	if s.micro != nil {
		_ = s.micro.Stop()
	}
	s.proj.Stop()
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
	if err := registry.RequireRole(ctx, s.proj.Meta(), r.Principal, contract.RoleAdmin, contract.RoleWriter, contract.RoleReader); err != nil {
		_ = req.Error("forbidden", err.Error(), nil)
		return
	}
	run, ok := s.proj.Serving().(*searchRun)
	if !ok {
		// Unreachable once Start has returned: the endpoint registers only
		// after the first fold catches up and swaps its run in.
		_ = req.Error("500", "index not caught up", nil)
		return
	}
	reply, err := run.query(r)
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
