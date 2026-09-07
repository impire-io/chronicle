// Package node runs chronicle-node: chronicle keeping the pattern's
// invariants for one tenant (chronicle-hq/02-DESIGN/04-fleet.md). It folds
// every log through an ordered consumer, maintains the STATE_<LOG> buckets
// under revision CAS, and serves the CHRON.API.> control verbs with
// registry role checks. It connects as that tenant's service user only.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/version"
)

// Config adjusts one node.
type Config struct {
	// Logger receives the fold's warnings — unknown op types, marked
	// payloads. Nil means slog.Default.
	Logger *slog.Logger
}

// Node is one running chronicle-node.
type Node struct {
	svc    micro.Service
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

// Stop stops the control verbs and the folds.
func (n *Node) Stop() {
	_ = n.svc.Stop()
	n.cancel()
	n.wg.Wait()
}

type node struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	meta   jetstream.KeyValue
	logger *slog.Logger

	foldCtx context.Context
	wg      *sync.WaitGroup

	mu    sync.Mutex
	folds map[string]bool
}

// Start opens the tenant's META bucket (provisioned at minting — a node
// without one is misplaced), starts a fold per known log, and registers
// the chronicle-node micro service. Stopping the returned node is the
// caller's job.
func Start(ctx context.Context, nc *nats.Conn, cfg Config) (*Node, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		return nil, fmt.Errorf("open META (provisioned at minting): %w", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	foldCtx, cancel := context.WithCancel(context.Background())
	n := &node{
		nc:      nc,
		js:      js,
		meta:    meta,
		logger:  logger,
		foldCtx: foldCtx,
		wg:      &sync.WaitGroup{},
		folds:   map[string]bool{},
	}

	// The logs live in META: log.<log>.config is the authoritative
	// inventory the node boots from.
	logs, err := n.listLogs(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	for _, log := range logs {
		if err := n.startFold(ctx, log); err != nil {
			cancel()
			return nil, fmt.Errorf("fold %s: %w", log, err)
		}
	}

	svc, err := micro.AddService(nc, micro.Config{
		Name:        "chronicle-node",
		Version:     version.Version,
		Description: "chronicle per-tenant node: fold, state buckets, CHRON.API verbs",
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("register service: %w", err)
	}
	endpoints := []struct {
		name    string
		subject string
		handler micro.HandlerFunc
	}{
		{"ping", client.PingSubject, n.handlePing},
		{"log-create", client.LogCreateSubject, n.handleLogCreate},
		{"schema-set", client.SchemaSetSubject, n.handleSchemaSet},
	}
	for _, e := range endpoints {
		if err := svc.AddEndpoint(e.name, e.handler, micro.WithEndpointSubject(e.subject)); err != nil {
			_ = svc.Stop()
			cancel()
			return nil, fmt.Errorf("add %s endpoint: %w", e.name, err)
		}
	}
	// Flush so the endpoint subscriptions have reached the server: once
	// Start returns, a request from any connection must find a responder.
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		_ = svc.Stop()
		cancel()
		return nil, fmt.Errorf("flush endpoint subscriptions: %w", err)
	}
	return &Node{svc: svc, cancel: cancel, wg: n.wg}, nil
}

func (n *node) handlePing(req micro.Request) {
	reply, err := json.Marshal(client.About{Name: "chronicle-node", Version: version.Version})
	if err != nil {
		_ = req.Error("500", "encode ping reply", nil)
		return
	}
	_ = req.Respond(reply)
}

func (n *node) handleLogCreate(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.LogCreateRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if err := n.requireRole(ctx, r.Principal, contract.RoleAdmin); err != nil {
		_ = req.Error("forbidden", err.Error(), nil)
		return
	}
	if err := contract.ValidateLogName(r.Log); err != nil {
		_ = req.Error("bad-log-name", err.Error(), nil)
		return
	}

	// The META key is the claim: create-if-absent, so two racing creates
	// settle without a lock.
	cfg, err := json.Marshal(contract.LogConfig{
		Status:      contract.LogStatusActive,
		Description: r.Description,
		MaxBytes:    r.MaxBytes,
	})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	if _, err := n.meta.Create(ctx, contract.MetaLogConfig(r.Log), cfg); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			_ = req.Error("log-exists", fmt.Sprintf("log %q already exists", r.Log), nil)
			return
		}
		_ = req.Error("500", err.Error(), nil)
		return
	}

	if _, err := n.js.CreateStream(ctx, contract.LogStreamConfig(r.Log, r.MaxBytes)); err != nil {
		_ = req.Error("500", fmt.Sprintf("create stream: %v", err), nil)
		return
	}
	if _, err := n.js.CreateKeyValue(ctx, contract.StateBucketConfig(r.Log)); err != nil {
		_ = req.Error("500", fmt.Sprintf("create state bucket: %v", err), nil)
		return
	}
	if err := n.startFold(ctx, r.Log); err != nil {
		_ = req.Error("500", fmt.Sprintf("start fold: %v", err), nil)
		return
	}

	reply, err := json.Marshal(client.LogCreateResponse{Stream: contract.StreamName(r.Log)})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

func (n *node) handleSchemaSet(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.SchemaSetRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if err := n.requireRole(ctx, r.Principal, contract.RoleAdmin); err != nil {
		_ = req.Error("forbidden", err.Error(), nil)
		return
	}
	if err := contract.ValidateLogName(r.Log); err != nil {
		_ = req.Error("bad-log-name", err.Error(), nil)
		return
	}
	if r.OpType == "" || r.OpType == contract.OpTypeSnapshot {
		_ = req.Error("bad-op-type", "op type must be non-empty and not the reserved snapshot type", nil)
		return
	}
	if _, err := n.meta.Get(ctx, contract.MetaLogConfig(r.Log)); err != nil {
		_ = req.Error("no-such-log", fmt.Sprintf("log %q is not created", r.Log), nil)
		return
	}
	// The schema must compile before it is declared: a vocabulary entry
	// nobody can validate against is noise.
	if _, err := client.CompileSchema(r.Schema); err != nil {
		_ = req.Error("bad-schema", err.Error(), nil)
		return
	}

	rev, err := n.recordSchema(ctx, r)
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	reply, err := json.Marshal(client.SchemaSetResponse{Revision: rev})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}
