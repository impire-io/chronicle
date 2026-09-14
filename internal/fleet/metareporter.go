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
	"github.com/impire-io/chronicle/internal/mint"
)

// metaIndexReporter is the report transport for a tenant whose node runs
// out-of-process (the microsandbox backend): the node's INDEX verbs write
// META but cannot reach the dispatch surface without the tenant-stamped
// account import — the multi-host increment. Until that bridge exists,
// the composition derives the reports from META itself: a watch on the
// tenant's index.> keys, translated into dispatch and stop calls. The
// initial replay is the boot re-derivation; the transport goes when the
// bridge lands.
type metaIndexReporter struct {
	tenant string
	nc     *nats.Conn
	report *indexReporter
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func startMetaIndexReporter(url, tenant string, serviceCreds []byte, dispatchConn *nats.Conn, logger *slog.Logger) (*metaIndexReporter, error) {
	nc, err := mint.ConnectCreds(url, serviceCreds, "chronicle-meta-reporter-"+tenant)
	if err != nil {
		return nil, fmt.Errorf("connect meta reporter for %s: %w", tenant, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	meta, err := js.KeyValue(ctx, contract.MetaBucket)
	if err != nil {
		cancel()
		nc.Close()
		return nil, fmt.Errorf("open META for %s: %w", tenant, err)
	}
	w, err := meta.Watch(ctx, contract.MetaIndexPrefix+">")
	if err != nil {
		cancel()
		nc.Close()
		return nil, fmt.Errorf("watch index declarations for %s: %w", tenant, err)
	}

	r := &metaIndexReporter{
		tenant: tenant,
		nc:     nc,
		report: &indexReporter{nc: dispatchConn, tenant: tenant, logger: logger},
		cancel: cancel,
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
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
				log, index, ok := splitIndexKey(entry.Key())
				if !ok {
					logger.Warn("index declaration key outside the grammar; ignored", "tenant", tenant, "key", entry.Key())
					continue
				}
				reportCtx, reportCancel := context.WithTimeout(ctx, 30*time.Second)
				switch entry.Operation() {
				case jetstream.KeyValuePut:
					var decl contract.IndexDeclaration
					if err := json.Unmarshal(entry.Value(), &decl); err != nil {
						logger.Warn("index declaration unreadable; ignored", "tenant", tenant, "key", entry.Key(), "err", err)
						reportCancel()
						continue
					}
					if err := r.report.IndexDeclared(reportCtx, log, index, decl.Kind); err != nil {
						logger.Warn("meta reporter: dispatch failed; the watch replay heals on restart", "tenant", tenant, "key", entry.Key(), "err", err)
					}
				case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
					if err := r.report.IndexDeleted(reportCtx, log, index); err != nil {
						logger.Warn("meta reporter: stop failed; the scan retires when the record catches up", "tenant", tenant, "key", entry.Key(), "err", err)
					}
				}
				reportCancel()
			}
		}
	}()
	return r, nil
}

func splitIndexKey(key string) (log, index string, ok bool) {
	rest := strings.TrimPrefix(key, contract.MetaIndexPrefix)
	parts := strings.SplitN(rest, ".", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func (r *metaIndexReporter) stop() {
	r.cancel()
	r.wg.Wait()
	r.nc.Close()
}
