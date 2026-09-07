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
		if reason := n.captureOp(ctx, log, op, &state, &sawSnapshot); reason != "" {
			return rollupResult{reason: reason}, nil
		}
	}
	if pending == 1 {
		// The history is one snapshot already: at birth shape, nothing
		// for a rollup to destroy.
		return rollupResult{reason: "nothing to compact"}, nil
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

// captureOp folds one replayed op into the in-memory state, judged by the
// shared core (0011): a snapshot resets, a schema-valid merge op applies.
// Everything the fold would warn about and skip is returned as a veto
// reason instead — what the fold could not capture, the node must not
// destroy.
func (n *node) captureOp(ctx context.Context, log string, op contract.Op, state *json.RawMessage, sawSnapshot *bool) string {
	if op.Type == contract.OpTypeSnapshot {
		snap, err := contract.ParseSnapshot(op.Payload)
		if err != nil {
			return fmt.Sprintf("op %s is a marked malformed snapshot", op.ID)
		}
		*state = snap.State
		*sawSnapshot = true
		return ""
	}
	switch decision, detail := foldcore.Judge(ctx, n.meta, log, op); decision {
	case foldcore.Merge:
		// Captured below.
	case foldcore.None:
		return fmt.Sprintf("op %s (type %s) declares effect none — its meaning lives only in history", op.ID, op.Type)
	case foldcore.UnknownType:
		return fmt.Sprintf("op %s has unknown type %s", op.ID, op.Type)
	case foldcore.UnknownEffect:
		return fmt.Sprintf("op %s (type %s) declares unknown effect: %s", op.ID, op.Type, detail)
	case foldcore.BadTypeRecord:
		return fmt.Sprintf("type record %s is unreadable: %s", op.Type, detail)
	case foldcore.Invalid:
		return fmt.Sprintf("op %s (type %s) is marked: %s", op.ID, op.Type, detail)
	}
	if !*sawSnapshot {
		return fmt.Sprintf("op %s lands before any snapshot (§ 5.1)", op.ID)
	}
	merged, err := contract.MergePatch(*state, op.Payload)
	if err != nil {
		return fmt.Sprintf("op %s (type %s) is marked: merge failed", op.ID, op.Type)
	}
	*state = merged
	return ""
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
