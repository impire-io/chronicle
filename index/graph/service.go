package graph

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

// Service is one running chronicle-index-graph instance.
type Service struct {
	proj  *projection.Projection
	micro micro.Service
}

// Start reads the index's own declaration from META — the edge rules are
// config, and config never hot-reloads: delete + declare is a workload
// lifecycle — then materializes over the shared projection spine and
// registers the query endpoint once caught up.
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
	if decl.Kind != contract.IndexKindGraph {
		return nil, fmt.Errorf("declaration %s/%s is kind %q, not graph", cfg.Log, cfg.Index, decl.Kind)
	}
	gcfg, err := contract.ParseGraphConfig(decl.Config)
	if err != nil {
		return nil, err
	}

	proj, err := projection.Start(ctx, nc, projection.Config{
		Log:    cfg.Log,
		Index:  cfg.Index,
		Kind:   "graph index",
		Logger: logger,
		NewRun: func() (projection.Run, error) { return newGraphRun(cfg.Log, gcfg.Edges, logger), nil },
	})
	if err != nil {
		return nil, err
	}
	s := &Service{proj: proj}

	m, err := micro.AddService(nc, micro.Config{
		Name:        "chronicle-index-graph",
		Version:     version.Version,
		Description: "chronicle graph index: declared edges over thing state",
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

// handleQuery answers the graph payload on the standard subject: the op
// field names the verb. Any registry role may query.
func (s *Service) handleQuery(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var r client.GraphQueryRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if err := registry.RequireRole(ctx, s.proj.Meta(), r.Principal, contract.RoleAdmin, contract.RoleWriter, contract.RoleReader); err != nil {
		_ = req.Error("forbidden", err.Error(), nil)
		return
	}
	if r.Thing == "" {
		_ = req.Error("bad-request", "thing is required", nil)
		return
	}
	switch r.Direction {
	case "", contract.GraphDirectionOut, contract.GraphDirectionIn, contract.GraphDirectionBoth:
	default:
		_ = req.Error("bad-direction", fmt.Sprintf("direction %q is not in the vocabulary (out, in, both)", r.Direction), nil)
		return
	}
	run, ok := s.proj.Serving().(*graphRun)
	if !ok {
		// Unreachable once Start has returned: the endpoint registers only
		// after the first fold catches up and swaps its run in.
		_ = req.Error("500", "index not caught up", nil)
		return
	}

	var reply any
	switch r.Op {
	case contract.GraphOpNeighbors, "":
		reply = run.neighbors(r)
	case contract.GraphOpWalk:
		reply = run.walk(r)
	default:
		// The kind-shaped payload rule (05-indexes.md § the query surface):
		// a payload outside the index's kind is refused with the kind named.
		_ = req.Error("bad-op", fmt.Sprintf("op %q is not in the graph kind's vocabulary (neighbors, walk)", r.Op), nil)
		return
	}
	data, err := json.Marshal(reply)
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(data)
}
