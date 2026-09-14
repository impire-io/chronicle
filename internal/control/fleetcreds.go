package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
)

// The record-verified creds pull (06-scheduler.md § the workload
// contract): the assigned executor asks for a workload's service creds,
// and the record is the authorization — no issuance ACL. Control verifies
// against STATE_FLEET, falling back to folding the workload's subject from
// the fleet log's tail when the bucket trails the assign. A release —
// including a liveness steal — revokes naturally: the old executor's pull
// no longer verifies.
//
// The executor field is caller-asserted for now, the same trust tier as
// Op-Author inside an account; cryptographic caller identity on this
// endpoint is the multi-host increment's named verify item (0014).

func (c *control) handleFleetCreds(req micro.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var r contract.FleetCredsRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		_ = req.Error("bad-request", err.Error(), nil)
		return
	}
	if r.Executor == "" || r.Tenant == "" || r.Workload == "" {
		_ = req.Error("bad-request", "executor, tenant, and workload are all required", nil)
		return
	}

	assigned, err := c.verifyAssignment(ctx, r)
	if err != nil {
		_ = req.Error("500", fmt.Sprintf("read the record: %v", err), nil)
		return
	}
	if !assigned {
		_ = req.Error("not-assigned", fmt.Sprintf("the record does not show %s carrying %s/%s", r.Executor, r.Tenant, r.Workload), nil)
		return
	}

	creds, err := os.ReadFile(filepath.Join(c.cfg.AccountsDir, r.Tenant, "service.creds"))
	if err != nil {
		_ = req.Error("500", fmt.Sprintf("read service creds: %v", err), nil)
		return
	}
	reply, err := json.Marshal(contract.FleetCredsResponse{Creds: creds})
	if err != nil {
		_ = req.Error("500", err.Error(), nil)
		return
	}
	_ = req.Respond(reply)
}

// verifyAssignment answers "does the record show this executor carrying
// this workload, right now?" — the bucket first, the log's tail when the
// bucket has no answer yet.
func (c *control) verifyAssignment(ctx context.Context, r contract.FleetCredsRequest) (bool, error) {
	thing := contract.FleetWorkloadThing(r.Tenant, r.Workload)

	if states, err := c.js.KeyValue(ctx, contract.StateBucket(contract.FleetLog)); err == nil {
		if entry, err := states.Get(ctx, thing); err == nil {
			var sv contract.StateValue
			if err := json.Unmarshal(entry.Value(), &sv); err == nil {
				if holds(sv.State, r.Executor) {
					return true, nil
				}
				// The bucket answered but not in the executor's favor; it
				// may simply trail the assign — the tail decides.
			}
		}
	}
	state, err := c.foldTail(ctx, thing)
	if err != nil {
		return false, err
	}
	return holds(state, r.Executor), nil
}

// holds checks the folded workload state for a live slot on the executor.
func holds(raw json.RawMessage, executor string) bool {
	if raw == nil {
		return false
	}
	var ws contract.WorkloadState
	if err := json.Unmarshal(raw, &ws); err != nil {
		return false
	}
	if ws.Stopped {
		return false
	}
	for _, slot := range ws.Slots {
		if slot.Executor == executor {
			return true
		}
	}
	return false
}

// foldTail folds one workload subject from the fleet log directly — the
// same rules as the standard fold, one subject, ephemeral consumer. The
// custody types are all merges and the writer is single, so the fold here
// stays deliberately small; junk is skipped exactly as the fold skips it.
func (c *control) foldTail(ctx context.Context, thing string) (json.RawMessage, error) {
	stream, err := c.js.Stream(ctx, contract.StreamName(contract.FleetLog))
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, nil // no fleet log yet: nothing is assigned
		}
		return nil, err
	}
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{contract.OpsSubject(contract.FleetLog, thing)},
	})
	if err != nil {
		return nil, err
	}

	// One bounded no-wait fetch: every Fetch on an ordered consumer resets
	// delivery to the start, so a drain loop would re-read forever. One
	// workload's custody tail is small by design — custody ops only, and
	// rollup keeps it compact — so the bound is generous, not load-bearing.
	batch, err := cons.FetchNoWait(1024)
	if err != nil {
		return nil, err
	}
	var state json.RawMessage
	for msg := range batch.Messages() {
		md, err := msg.Metadata()
		if err != nil {
			continue
		}
		op := contract.ParseOp(msg.Subject(), md.Sequence.Stream, msg.Headers(), msg.Data())
		switch op.Type {
		case contract.OpTypeSnapshot:
			if snap, err := contract.ParseSnapshot(op.Payload); err == nil {
				state = snap.State
			}
		case contract.FleetOpAssign, contract.FleetOpRelease, contract.FleetOpStop:
			if state == nil {
				continue // malformed for state until a snapshot appears
			}
			if merged, err := contract.MergePatch(state, op.Payload); err == nil {
				state = merged
			}
		}
	}
	if batch.Error() != nil {
		return state, batch.Error()
	}
	return state, nil
}
