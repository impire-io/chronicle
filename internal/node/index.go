package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/contract"
)

// handleIndexDeclare declares an index (05-indexes.md): one META key,
// create-if-absent, so two racing declares settle without a lock. The kind
// is checked against this node's vocabulary — write-side strictness, the
// same split effects got in 0011; a supervisor reading a newer build's
// declaration stays tolerant on its side.
func (n *node) handleIndexDeclare(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.IndexDeclareRequest
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
	if err := contract.ValidateIndexName(r.Index); err != nil {
		_ = req.Error("bad-index-name", err.Error(), nil)
		return
	}
	// The state kind is the one exception (0023): its declaration is
	// written by the node at log creation and only there.
	if r.Kind == contract.IndexKindState || r.Index == contract.StateIndexName {
		_ = req.Error("reserved-state-index", "the state index is declared at log creation and only there (0023)", nil)
		return
	}
	if !contract.KnownIndexKind(r.Kind) {
		_ = req.Error("bad-kind", fmt.Sprintf("kind %q is not in this node's vocabulary (search, graph, semantic)", r.Kind), nil)
		return
	}
	// Config belongs to the kind — write-side strict (0015): graph
	// requires well-formed edge rules and stays state-only (0020 § 4);
	// search and semantic admit the source (0020) and its narrowing.
	switch r.Kind {
	case contract.IndexKindSearch:
		if _, err := contract.ParseSearchConfig(r.Config); err != nil {
			_ = req.Error("bad-config", err.Error(), nil)
			return
		}
	case contract.IndexKindGraph:
		if _, err := contract.ParseGraphConfig(r.Config); err != nil {
			_ = req.Error("bad-config", err.Error(), nil)
			return
		}
	case contract.IndexKindSemantic:
		if _, err := contract.ParseSemanticConfig(r.Config); err != nil {
			_ = req.Error("bad-config", err.Error(), nil)
			return
		}
	default:
		if len(r.Config) > 0 {
			_ = req.Error("bad-config", fmt.Sprintf("kind %q takes no config", r.Kind), nil)
			return
		}
	}
	if _, err := n.meta.Get(ctx, contract.MetaLogConfig(r.Log)); err != nil {
		_ = req.Error("no-such-log", fmt.Sprintf("log %q is not created", r.Log), nil)
		return
	}

	value, err := json.Marshal(contract.IndexDeclaration{Kind: r.Kind, Config: r.Config})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	if _, err := n.meta.Create(ctx, contract.MetaIndex(r.Log, r.Index), value); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			_ = req.Error("index-exists", fmt.Sprintf("index %q on log %q already exists", r.Index, r.Log), nil)
			return
		}
		_ = req.Error("500", err.Error(), nil)
		return
	}
	// The declaration stands regardless of the report: boot re-derivation
	// heals a report that failed, and no skew can grow while the node is
	// down because declarations only happen through the node.
	if n.indexes != nil {
		if err := n.indexes.IndexDeclared(ctx, r.Log, r.Index, r.Kind); err != nil {
			n.logger.Warn("index declared but the report failed; boot re-derivation heals it", "log", r.Log, "index", r.Index, "err", err)
		}
	}

	reply, err := json.Marshal(client.IndexDeclareResponse{Query: client.IndexQuerySubject(r.Log, r.Index)})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// handleIndexDelete retires an index: the META key goes and the
// supervisor stops the workload. Derived only — nothing of record is
// lost, and re-declaring rebuilds it by replay.
func (n *node) handleIndexDelete(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var r client.IndexDeleteRequest
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
	if err := contract.ValidateIndexName(r.Index); err != nil {
		_ = req.Error("bad-index-name", err.Error(), nil)
		return
	}
	// The exactness recipe and roll-up consume the state index: it is
	// the one derived view whose loss would break a contract (0023).
	if r.Index == contract.StateIndexName {
		_ = req.Error("reserved-state-index", "the state index cannot be deleted while the log exists (0023)", nil)
		return
	}
	// Get first: deleting an absent key succeeds silently in KV, and the
	// caller deserves the honest answer.
	if _, err := n.meta.Get(ctx, contract.MetaIndex(r.Log, r.Index)); err != nil {
		_ = req.Error("no-such-index", fmt.Sprintf("index %q on log %q is not declared", r.Index, r.Log), nil)
		return
	}
	if err := n.meta.Delete(ctx, contract.MetaIndex(r.Log, r.Index)); err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	if n.indexes != nil {
		if err := n.indexes.IndexDeleted(ctx, r.Log, r.Index); err != nil {
			n.logger.Warn("index deleted but the report failed; the scan retires the workload when the record catches up", "log", r.Log, "index", r.Index, "err", err)
		}
	}

	reply, err := json.Marshal(client.IndexDeleteResponse{Deleted: true})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// rederiveIndexes reports every declared index — the boot half of the
// level-triggered healing: idempotent at the dispatch surface, so a lost
// report or a stale record converges on every node start. Every failure
// is a warning: declarations are the truth and the node serves them
// whether or not anyone is listening for reports.
func (n *node) rederiveIndexes(ctx context.Context) {
	lister, err := n.meta.ListKeysFiltered(ctx, contract.MetaIndexPrefix+">")
	if err != nil {
		n.logger.Warn("re-derive: list index declarations", "err", err)
		return
	}
	for key := range lister.Keys() {
		rest := strings.TrimPrefix(key, contract.MetaIndexPrefix)
		parts := strings.SplitN(rest, ".", 2)
		if len(parts) != 2 {
			n.logger.Warn("index declaration key outside the grammar; ignored", "key", key)
			continue
		}
		entry, err := n.meta.Get(ctx, key)
		if err != nil {
			continue
		}
		var decl contract.IndexDeclaration
		if err := json.Unmarshal(entry.Value(), &decl); err != nil {
			n.logger.Warn("index declaration unreadable; ignored", "key", key, "err", err)
			continue
		}
		if decl.Kind == contract.IndexKindState {
			// The state index places no workload — it rides the node
			// (0023), so there is nothing to report.
			continue
		}
		if err := n.indexes.IndexDeclared(ctx, parts[0], parts[1], decl.Kind); err != nil {
			n.logger.Warn("re-derive: report failed; the next boot heals", "key", key, "err", err)
		}
	}
}

// bridgeReporter is the node's default report transport: the
// tenant-stamped bridge (06-scheduler.md § the dispatch surface),
// published over the node's own connection — the import in the account
// JWT maps the local subject to the stamped form, so the same code runs
// in-process, in a microVM, or on another machine.
type bridgeReporter struct {
	nc *nats.Conn
}

func (r *bridgeReporter) report(ctx context.Context, report contract.FleetIndexReport) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	msg, err := r.nc.RequestWithContext(reqCtx, contract.FleetBridgeLocalSubject, data)
	if err != nil {
		return err
	}
	if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
		return fmt.Errorf("%s: %s", code, msg.Header.Get(micro.ErrorHeader))
	}
	return nil
}

func (r *bridgeReporter) IndexDeclared(ctx context.Context, log, index, kind string) error {
	return r.report(ctx, contract.FleetIndexReport{Action: contract.IndexReportDeclared, Log: log, Index: index, Kind: kind})
}

func (r *bridgeReporter) IndexDeleted(ctx context.Context, log, index string) error {
	return r.report(ctx, contract.FleetIndexReport{Action: contract.IndexReportDeleted, Log: log, Index: index})
}
