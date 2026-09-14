package semantic

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
	"github.com/impire-io/chronicle/internal/index/projection"
	"github.com/impire-io/chronicle/internal/registry"
	"github.com/impire-io/chronicle/internal/version"
)

// Config names the one index this service materializes and the provider
// the install carries.
type Config struct {
	Log      string
	Index    string
	Provider ProviderConfig
	// ChunkBytes overrides the chunk budget; zero means the contract
	// default. A byte budget the operator corrects per model —
	// deliberately not a tokenizer.
	ChunkBytes int
	// Logger receives the fold's warnings; nil means slog.Default.
	Logger *slog.Logger
}

// Service is one running chronicle-index-semantic instance.
type Service struct {
	proj  *projection.Projection
	micro micro.Service
}

// Start probes the provider — it hides nothing, so ask it early and say
// plainly when it is absent — reads the index's declaration, folds over
// the shared spine, and registers the query endpoint only after the fold
// *and* the initial embed pass: a responder implies a current index, and
// for the semantic kind current means embedded (0016).
func Start(ctx context.Context, nc *nats.Conn, cfg Config) (*Service, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if !cfg.Provider.Configured() {
		return nil, fmt.Errorf("the semantic kind needs an embedding provider (base url and model) — install configuration, absent here")
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
	if decl.Kind != contract.IndexKindSemantic {
		return nil, fmt.Errorf("declaration %s/%s is kind %q, not semantic", cfg.Log, cfg.Index, decl.Kind)
	}
	scfg, err := contract.ParseSemanticConfig(decl.Config)
	if err != nil {
		return nil, err
	}
	model := scfg.Model
	if model == "" {
		model = cfg.Provider.Model
	}

	prov := newProvider(cfg.Provider)
	if err := prov.Probe(ctx, model); err != nil {
		return nil, fmt.Errorf("the embedding provider is not answering: %w", err)
	}

	proj, err := projection.Start(ctx, nc, projection.Config{
		Log:    cfg.Log,
		Index:  cfg.Index,
		Kind:   "semantic index",
		Logger: logger,
		NewRun: func() (projection.Run, error) {
			return newSemanticRun(cfg.Log, prov, model, scfg.Fields, cfg.ChunkBytes, logger), nil
		},
	})
	if err != nil {
		return nil, err
	}
	s := &Service{proj: proj}

	// The initial embed pass: the fold is caught up; wait until the
	// worker has drained it once. A dead provider means no responder —
	// the same honest silence a replaying index gives.
	for {
		run, ok := proj.Serving().(*semanticRun)
		if ok && run.unembedded() == 0 {
			break
		}
		select {
		case <-ctx.Done():
			proj.Stop()
			return nil, fmt.Errorf("initial embed pass: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	m, err := micro.AddService(nc, micro.Config{
		Name:        "chronicle-index-semantic",
		Version:     version.Version,
		Description: "chronicle semantic index: meaning over thing state through the install's embedding provider",
		Metadata:    map[string]string{"log": cfg.Log, "index": cfg.Index, "model": model},
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
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = m.Stop()
		proj.Stop()
		return nil, fmt.Errorf("flush endpoint subscription: %w", err)
	}
	s.micro = m
	return s, nil
}

// Stop takes the endpoint off the wire, then stops the projection (which
// closes the run and its embed worker).
func (s *Service) Stop() {
	if s.micro != nil {
		_ = s.micro.Stop()
	}
	s.proj.Stop()
}

// handleQuery answers the semantic payload on the standard subject. Any
// registry role may query; the reply carries the unembedded count — a
// degraded index says so instead of pretending completeness.
func (s *Service) handleQuery(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.SemanticQueryRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if err := registry.RequireRole(ctx, s.proj.Meta(), r.Principal, contract.RoleAdmin, contract.RoleWriter, contract.RoleReader); err != nil {
		_ = req.Error("forbidden", err.Error(), nil)
		return
	}
	if r.Text == "" {
		_ = req.Error("bad-request", "text is required", nil)
		return
	}
	run, ok := s.proj.Serving().(*semanticRun)
	if !ok {
		// Unreachable once Start has returned.
		_ = req.Error("500", "index not caught up", nil)
		return
	}
	reply, err := run.query(ctx, r)
	if err != nil {
		_ = req.Error("provider-unavailable", fmt.Sprintf("embed the query: %v", err), nil)
		return
	}
	data, err := json.Marshal(reply)
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(data)
}
