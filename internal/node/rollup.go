package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/foldcore"
)

// rollupAuthor is the Op-Author the node stamps on its own snapshots —
// testimony, like every author claim.
const rollupAuthor = "chronicle-node"

// errNoThing is a rollup asked of a subject with no history at all.
var errNoThing = errors.New("no history on the subject")

// rollupResult is one rollup attempt's outcome: rolled at seq, or the
// reason the node declined — a gate veto, an empty tail, a lost race.
type rollupResult struct {
	rolled bool
	seq    uint64
	reason string
}

// rollupThing compacts one exact subject, if the effect gate allows it:
// replay the subject, fold it under the current declarations (0011), and
// publish the result as a rollup snapshot guarded by the last replayed
// seq — race-safe against anyone else's rollup, including an SDK save
// (pattern § 5.2: first writer wins, the loser discards).
//
// The gate, at its strictest: rollup destroys every earlier message on
// the subject, so the node compacts only history its fold fully captured
// into state. Any message it could not capture — an effect-none op, an
// unknown type, an unknown effect value, a marked (schema-invalid) op,
// an op before the first snapshot — vetoes, and compaction stays the
// application's call (decision 0011 § 4).
func (n *node) rollupThing(ctx context.Context, log, thing string) (rollupResult, error) {
	mu := n.logMutex(log)
	mu.Lock()
	defer mu.Unlock()

	// The log-level gate sits above the effect gate (0019): a preserved
	// log's trail is the product, and the node never compacts any of it.
	// Read-side tolerant — an unreadable config reads as compactable; the
	// stream's own AllowRollup refusal stays the guarantee regardless.
	if entry, err := n.meta.Get(ctx, contract.MetaLogConfig(log)); err == nil {
		var cfg contract.LogConfig
		if jerr := json.Unmarshal(entry.Value(), &cfg); jerr == nil &&
			contract.NormalizeHistory(cfg.History) == contract.HistoryPreserved {
			return rollupResult{reason: "the log declares history preserved (0019)"}, nil
		}
	}

	// The thing's type gates second (0022 § 4): on a compactable log a
	// typed thing compacts or is skipped by its type's history facet —
	// node-honored, the soft tier; there is no per-subject AllowRollup.
	// The per-op effect veto (0011 § 4) survives only where no type
	// resolves; a marked undeclared aspect is declined outright.
	res, err := foldcore.ResolveThing(ctx, n.meta, log, thing)
	if err != nil {
		return rollupResult{}, fmt.Errorf("resolve thing: %w", err)
	}
	switch res.Kind {
	case contract.ResolvedUndeclared:
		return rollupResult{reason: fmt.Sprintf("the subject is a marked undeclared aspect: %s", res.Detail)}, nil
	case contract.ResolvedTyped:
		if contract.NormalizeHistory(res.Record.History) == contract.HistoryPreserved {
			return rollupResult{reason: fmt.Sprintf("the thing's type %q declares history preserved (0022)", res.TypeName)}, nil
		}
	}

	subject := contract.OpsSubject(log, thing)
	stream, err := n.js.Stream(ctx, contract.StreamName(log))
	if err != nil {
		return rollupResult{}, fmt.Errorf("open stream: %w", err)
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
	})
	if err != nil {
		return rollupResult{}, fmt.Errorf("ordered consumer: %w", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return rollupResult{}, fmt.Errorf("consumer info: %w", err)
	}
	pending := info.NumPending
	if pending == 0 {
		return rollupResult{}, errNoThing
	}

	var (
		state       json.RawMessage
		sawSnapshot bool
		frontier    = []string{}
		lastSeq     uint64
	)
	for range pending {
		msg, err := cons.Next(jetstream.FetchMaxWait(10 * time.Second))
		if err != nil {
			return rollupResult{}, fmt.Errorf("replay next: %w", err)
		}
		md, err := msg.Metadata()
		if err != nil {
			return rollupResult{}, fmt.Errorf("replay metadata: %w", err)
		}
		op := contract.ParseOp(msg.Subject(), md.Sequence.Stream, msg.Headers(), msg.Data())
		lastSeq = op.Seq
		if op.ID != "" {
			frontier = []string{op.ID}
		}
		if res.Kind == contract.ResolvedTyped {
			captureTyped(res.Record, op, &state, &sawSnapshot)
		} else if reason := captureUntyped(op, &state, &sawSnapshot); reason != "" {
			return rollupResult{reason: reason}, nil
		}
	}
	if pending == 1 {
		// The history is one snapshot already: at birth shape, nothing
		// for a rollup to destroy.
		return rollupResult{reason: "nothing to compact"}, nil
	}
	if !sawSnapshot {
		// Even absorption needs a floor: with no valid snapshot the fold
		// derived nothing, and a rollup would replace history with a
		// state that never existed.
		return rollupResult{reason: "no valid snapshot on the subject to fold from"}, nil
	}

	payload, err := json.Marshal(contract.Snapshot{State: state, Frontier: frontier})
	if err != nil {
		return rollupResult{}, fmt.Errorf("snapshot payload: %w", err)
	}
	msg := nats.NewMsg(subject)
	msg.Header = contract.Op{
		ID:     nuid.Next(),
		Type:   contract.OpTypeSnapshot,
		Author: rollupAuthor,
		Ts:     time.Now(),
	}.Header()
	msg.Header.Set(contract.HdrRollup, contract.RollupSubject)
	msg.Header.Set(contract.HdrExpectedLastSubjSeq, strconv.FormatUint(lastSeq, 10))
	msg.Data = payload

	ack, err := n.js.PublishMsg(ctx, msg)
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			// Someone else's message landed after the replay — first
			// writer wins, this attempt discards, nothing to clean up.
			return rollupResult{reason: "lost the race: the log moved past the replayed history"}, nil
		}
		return rollupResult{}, fmt.Errorf("publish rollup: %w", err)
	}
	return rollupResult{rolled: true, seq: ack.Sequence}, nil
}

// captureTyped folds one replayed op of a typed, compactable thing. No
// per-op veto (0022 § 5): the type's declaration made this history
// absorbable, so anything the fold would mark or skip is absorbed — the
// rollup snapshot keeps exactly what folded state keeps. Ops whose
// meaning must survive belong on a preserved aspect instead.
func captureTyped(rec *contract.TypeRecord, op contract.Op, state *json.RawMessage, sawSnapshot *bool) {
	if op.Type == contract.OpTypeSnapshot {
		snap, err := contract.ParseSnapshot(op.Payload)
		if err != nil {
			return // marked; absorbed
		}
		if foldcore.JudgeSnapshot(rec, snap.State) != "" {
			return // marked; absorbed
		}
		*state = snap.State
		*sawSnapshot = true
		return
	}
	if decision, _ := foldcore.JudgeRecord(rec, op); decision != foldcore.Merge || !*sawSnapshot {
		return // none, unknown, marked, or pre-snapshot; absorbed
	}
	if merged, err := contract.MergePatch(*state, op.Payload); err == nil {
		*state = merged
	}
}

// captureUntyped keeps the per-op veto for untyped things (0011 § 4,
// unchanged): what the fold could not capture, the node must not destroy.
// An untyped thing's non-snapshot ops all judge unknown — the
// vocabulary-less floor — so only snapshot-shaped histories compact.
func captureUntyped(op contract.Op, state *json.RawMessage, sawSnapshot *bool) string {
	if op.Type == contract.OpTypeSnapshot {
		snap, err := contract.ParseSnapshot(op.Payload)
		if err != nil {
			return fmt.Sprintf("op %s is a marked malformed snapshot", op.ID)
		}
		*state = snap.State
		*sawSnapshot = true
		return ""
	}
	return fmt.Sprintf("op %s (type %s) has no declaration — the thing is untyped, its meaning lives in history", op.ID, op.Type)
}

// rollupTimer is the timer trigger: every rollupEvery it sweeps the
// subjects the folds saw ops on since the last sweep.
func (n *node) rollupTimer() {
	defer n.wg.Done()
	t := time.NewTicker(n.rollupEvery)
	defer t.Stop()
	for {
		select {
		case <-n.foldCtx.Done():
			return
		case <-t.C:
			n.rollupActive()
		}
	}
}

// rollupActive runs one sweep. A veto or a lost race only skips; the
// subject re-enters the active set on its next op.
func (n *node) rollupActive() {
	n.mu.Lock()
	runs := make(map[string]*foldRun, len(n.folds))
	for log, run := range n.folds {
		runs[log] = run
	}
	n.mu.Unlock()
	for log, run := range runs {
		for _, thing := range run.f.swapActive() {
			ctx, cancel := context.WithTimeout(n.foldCtx, 30*time.Second)
			res, err := n.rollupThing(ctx, log, thing)
			cancel()
			switch {
			case err != nil:
				n.logger.Warn("rollup: sweep attempt failed", "log", log, "thing", thing, "err", err)
			case res.rolled:
				n.logger.Info("rollup: compacted", "log", log, "thing", thing, "seq", res.seq)
			default:
				n.logger.Debug("rollup: skipped", "log", log, "thing", thing, "reason", res.reason)
			}
		}
	}
}
