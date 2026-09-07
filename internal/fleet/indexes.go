package fleet

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
	"github.com/impire-io/chronicle/internal/index/search"
)

// tenantIndexes supervises one tenant's declared indexes in-process: a
// watcher on META's index.> keys starts and stops indexer workloads to
// match the declarations — the scheduler-less stand-in for 0004's
// scheduler (05-indexes.md § who runs it), sitting exactly where it will.
// All of the tenant's indexers share one connection, holding that
// tenant's service user only.
type tenantIndexes struct {
	tenant string
	nc     *nats.Conn
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu sync.Mutex
	// wanted is the declared state, running the realized one; a start
	// that finishes after its declaration was retired stops itself.
	wanted  map[string]bool
	running map[string]*search.Service
}

// startTenantIndexes opens the tenant's META and begins supervising. The
// watcher's initial replay realizes every declaration already on record.
func startTenantIndexes(nc *nats.Conn, tenant string, logger *slog.Logger) (*tenantIndexes, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open META: %w", err)
	}
	w, err := meta.Watch(ctx, contract.MetaIndexPrefix+">")
	if err != nil {
		cancel()
		return nil, fmt.Errorf("watch index declarations: %w", err)
	}

	ti := &tenantIndexes{
		tenant:  tenant,
		nc:      nc,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
		wanted:  map[string]bool{},
		running: map[string]*search.Service{},
	}
	ti.wg.Add(1)
	go func() {
		defer ti.wg.Done()
		defer func() { _ = w.Stop() }()
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-w.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue // the initial replay marker
				}
				switch entry.Operation() {
				case jetstream.KeyValuePut:
					ti.declare(entry.Key(), entry.Value())
				case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
					ti.retire(entry.Key())
				}
			}
		}
	}()
	return ti, nil
}

// declare realizes one declaration. Catching up with the log can take a
// while, so the indexer starts off the watcher goroutine — declarations
// keep being served while one index replays.
func (ti *tenantIndexes) declare(key string, value []byte) {
	rest := strings.TrimPrefix(key, contract.MetaIndexPrefix)
	parts := strings.SplitN(rest, ".", 2)
	if len(parts) != 2 {
		ti.logger.Warn("index declaration key outside the grammar; ignored", "tenant", ti.tenant, "key", key)
		return
	}
	log, index := parts[0], parts[1]
	var decl contract.IndexDeclaration
	if err := json.Unmarshal(value, &decl); err != nil {
		ti.logger.Warn("index declaration unreadable; ignored", "tenant", ti.tenant, "key", key, "err", err)
		return
	}
	if decl.Kind != contract.IndexKindSearch {
		// Read-side tolerance: a newer build may declare kinds this one
		// has no workload for.
		ti.logger.Warn("unknown index kind ignored", "tenant", ti.tenant, "key", key, "kind", decl.Kind)
		return
	}

	ti.mu.Lock()
	if ti.wanted[key] {
		ti.mu.Unlock()
		return // already realized or starting; declare is create-only
	}
	ti.wanted[key] = true
	ti.mu.Unlock()

	ti.wg.Add(1)
	go func() {
		defer ti.wg.Done()
		startCtx, cancel := context.WithTimeout(ti.ctx, time.Minute)
		defer cancel()
		svc, err := search.Start(startCtx, ti.nc, search.Config{Log: log, Index: index, Logger: ti.logger})
		if err != nil {
			ti.logger.Warn("start indexer", "tenant", ti.tenant, "log", log, "index", index, "err", err)
			ti.mu.Lock()
			delete(ti.wanted, key)
			ti.mu.Unlock()
			return
		}
		ti.mu.Lock()
		if !ti.wanted[key] {
			// Retired while starting.
			ti.mu.Unlock()
			svc.Stop()
			return
		}
		ti.running[key] = svc
		ti.mu.Unlock()
		ti.logger.Info("index serving", "tenant", ti.tenant, "log", log, "index", index)
	}()
}

// retire stops one index's workload. Derived only: nothing to clean up
// beyond the process.
func (ti *tenantIndexes) retire(key string) {
	ti.mu.Lock()
	delete(ti.wanted, key)
	svc := ti.running[key]
	delete(ti.running, key)
	ti.mu.Unlock()
	if svc != nil {
		svc.Stop()
		ti.logger.Info("index retired", "tenant", ti.tenant, "key", key)
	}
}

// stop tears the supervisor down: watcher first, then every workload.
func (ti *tenantIndexes) stop() {
	ti.cancel()
	ti.mu.Lock()
	running := ti.running
	ti.running = map[string]*search.Service{}
	ti.wanted = map[string]bool{}
	ti.mu.Unlock()
	for _, svc := range running {
		svc.Stop()
	}
	ti.wg.Wait()
}
