package contract

import (
	"encoding/json"
	"fmt"
)

// The fleet log (chronicle-hq/02-DESIGN/06-scheduler.md, decision 0014):
// workload scheduling as a chronicle log in the control account. Two thing
// families share one stream and one total order, so every placement
// decision cites a single sequence number as its knowledge horizon. The
// vocabulary is custody-only — local restarts and health never touch the
// record — and chronicle-workloads is the log's only writer.

// FleetLog is the log's name in the control account: stream LOG_FLEET,
// state bucket STATE_FLEET, subjects CHRON.fleet.>.
const FleetLog = "fleet"

// The thing families. The family tokens are reserved within the fleet log;
// a third family is a design amendment, not an ad-hoc subject.
const (
	fleetWorkloadFamily = "workload."
	fleetExecutorFamily = "executor."
)

// FleetWorkloadThing is the thing tail of one workload's custody record:
// workload.<tenant>.<workload>.
func FleetWorkloadThing(tenant, workload string) string {
	return fleetWorkloadFamily + tenant + "." + workload
}

// FleetExecutorThing is the thing tail of one executor's roster record:
// executor.<executor>.
func FleetExecutorThing(executor string) string {
	return fleetExecutorFamily + executor
}

// The custody op types — merge effects on the workload thing, landed only
// by chronicle-workloads, always under the expected-sequence guard. Birth
// is the pattern's own snapshot type (§5.1): "dispatch" and "register" are
// the API verbs whose record is that snapshot.
const (
	FleetOpAssign  = "workload.assign"
	FleetOpRelease = "workload.release"
	FleetOpStop    = "workload.stop"
)

// The workload kinds backend zero runs. The kind names the materializer;
// the OCI image arrives with the first 0004 backend.
const (
	WorkloadKindNode        = "node"
	WorkloadKindIndexSearch = "index-search"
)

// WorkloadNodeName is the one node workload's name within its tenant.
const WorkloadNodeName = "node"

// WorkloadIndexName is an indexer workload's name within its tenant:
// index-<log>-<index>. Log and index names admit no dots, so the name is a
// single subject token.
func WorkloadIndexName(log, index string) string {
	return "index-" + log + "-" + index
}

// WorkloadSlot is one filled slot: which executor carries it.
type WorkloadSlot struct {
	Executor string `json:"executor"`
}

// WorkloadFailures rides the merged state — never erasable history — so
// compaction can be embraced without losing what supervision needs
// (06-scheduler.md § the fleet log).
type WorkloadFailures struct {
	Count      int    `json:"count"`
	LastReason string `json:"last_reason,omitempty"`
}

// WorkloadState is the workload thing's materialised state: the dispatch
// snapshot plus the custody merges. Slots are keyed "0".."replicas-1".
type WorkloadState struct {
	Kind     string `json:"kind"`
	Tenant   string `json:"tenant"`
	Log      string `json:"log,omitempty"`
	Index    string `json:"index,omitempty"`
	Replicas int    `json:"replicas"`

	Slots    map[string]WorkloadSlot `json:"slots"`
	Failures *WorkloadFailures       `json:"failures,omitempty"`
	Stopped  bool                    `json:"stopped,omitempty"`
}

// ExecutorState is the executor thing's materialised state: the register
// snapshot. Tags and capacity arrive with consumers that read them.
type ExecutorState struct {
	Backend string `json:"backend"`
}

// The release reasons — why a slot opened (06-scheduler.md § replicas and
// supervision).
const (
	ReleaseFailing  = "failing"  // the owning executor exhausted its restart budget
	ReleaseLiveness = "liveness" // the executor stopped answering; the slot was taken back
	ReleaseRestart  = "restart"  // the executor is alive but the placement is gone
	ReleaseStopped  = "stopped"  // the workload was retired
)

// The control-plane fleet subjects, under the cross-account CHRON.CTRL.>
// surface. DISPATCH, STOP, REGISTER and REPORT are chronicle-workloads'
// queue-grouped endpoints; CREDS is chronicle-control's; AUCTION is a plain
// subscription in every executor — deliberately not a queue group, every
// executor must hear the scatter; DELEGATE/STATUS/DESTROY are per-executor.
const (
	FleetDispatchSubject = "CHRON.CTRL.FLEET.DISPATCH"
	FleetStopSubject     = "CHRON.CTRL.FLEET.STOP"
	FleetRegisterSubject = "CHRON.CTRL.FLEET.REGISTER"
	FleetReportSubject   = "CHRON.CTRL.FLEET.REPORT"
	FleetCredsSubject    = "CHRON.CTRL.FLEET.CREDS"
	FleetAuctionSubject  = "CHRON.CTRL.FLEET.AUCTION"
)

// FleetDelegateSubject is one executor's delegation endpoint.
func FleetDelegateSubject(executor string) string {
	return "CHRON.CTRL.FLEET.DELEGATE." + executor
}

// FleetStatusSubject is one executor's placement-status endpoint — the
// backend witness of the level scan's two.
func FleetStatusSubject(executor string) string {
	return "CHRON.CTRL.FLEET.STATUS." + executor
}

// FleetDestroySubject is one executor's destroy endpoint.
func FleetDestroySubject(executor string) string {
	return "CHRON.CTRL.FLEET.DESTROY." + executor
}

// FleetDispatchRequest asks for a workload to exist. Replicas zero means 1.
type FleetDispatchRequest struct {
	Tenant   string `json:"tenant"`
	Workload string `json:"workload"`
	Kind     string `json:"kind"`
	Log      string `json:"log,omitempty"`
	Index    string `json:"index,omitempty"`
	Replicas int    `json:"replicas,omitempty"`
}

// FleetDispatchResponse acknowledges the record. Dispatching an existing
// workload is a no-op — birth's zero guard settles it without a lock.
type FleetDispatchResponse struct {
	Dispatched bool `json:"dispatched"`
	// Existed reports the idempotent case: the record was already born.
	Existed bool `json:"existed,omitempty"`
}

// FleetStopRequest retires a workload: assigned executors destroy, slots
// release with reason stopped.
type FleetStopRequest struct {
	Tenant   string `json:"tenant"`
	Workload string `json:"workload"`
}

// FleetStopResponse acknowledges the retirement record.
type FleetStopResponse struct {
	Stopped bool `json:"stopped"`
}

// FleetRegisterRequest puts an executor on the roster. Re-registering is
// idempotent — a fresh boot of the same executor is the normal case.
type FleetRegisterRequest struct {
	Executor string `json:"executor"`
	Backend  string `json:"backend"`
}

// FleetRegisterResponse acknowledges the roster record.
type FleetRegisterResponse struct {
	Registered bool `json:"registered"`
}

// FleetReportRequest is an executor's custody report: this slot is gone
// for good (the restart budget is exhausted). Local restarts are not
// reported — custody is unchanged.
type FleetReportRequest struct {
	Executor string `json:"executor"`
	Tenant   string `json:"tenant"`
	Workload string `json:"workload"`
	Slot     string `json:"slot"`
	Reason   string `json:"reason"`
}

// FleetReportResponse acknowledges the release record.
type FleetReportResponse struct {
	Released bool `json:"released"`
}

// FleetAuctionRequest is the scatter: who can carry this workload? Bids
// travel on the reply inbox within the gather window; nothing here touches
// the log's capture.
type FleetAuctionRequest struct {
	Tenant   string `json:"tenant"`
	Workload string `json:"workload"`
	Kind     string `json:"kind"`
}

// FleetAuctionBid is one executor's live self-assessment: replying at all
// confirms capability; Placements is the live load, fewer is better.
type FleetAuctionBid struct {
	Executor   string `json:"executor"`
	Placements int    `json:"placements"`
}

// FleetDelegateRequest hands one slot to the bidder that won. The executor
// accepts or refuses — a bid is a promise made on live state, and by
// delegation time it may be stale. Only after the accept does the assign
// land, under the guard.
type FleetDelegateRequest struct {
	Tenant   string `json:"tenant"`
	Workload string `json:"workload"`
	Slot     string `json:"slot"`
	Kind     string `json:"kind"`
	Log      string `json:"log,omitempty"`
	Index    string `json:"index,omitempty"`
}

// FleetDelegateResponse is the accept or the refusal.
type FleetDelegateResponse struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// FleetStatusRequest asks the backend witness about one placement.
type FleetStatusRequest struct {
	Tenant   string `json:"tenant"`
	Workload string `json:"workload"`
}

// The status vocabulary the backend witness answers with. "not-found" from
// a live executor means the placement is gone — released and re-auctioned;
// no responder at all means the executor is gone — same custody outcome,
// different reason on the record.
const (
	PlacementNotFound = "not-found"
	PlacementStarting = "starting"
	PlacementRunning  = "running"
)

// FleetStatusResponse is the backend witness's answer.
type FleetStatusResponse struct {
	Status string `json:"status"`
}

// FleetDestroyRequest tells an executor to tear one placement down.
// Destroying the absent is success, not error — the seam is idempotent.
type FleetDestroyRequest struct {
	Tenant   string `json:"tenant"`
	Workload string `json:"workload"`
}

// FleetDestroyResponse acknowledges absence.
type FleetDestroyResponse struct {
	Destroyed bool `json:"destroyed"`
}

// FleetCredsRequest is the record-verified pull (06-scheduler.md § the
// workload contract): the assigned executor asks chronicle-control for the
// workload's service creds. The record is the authorization — control
// verifies the assignment against STATE_FLEET, falling back to folding the
// workload's subject from the log's tail when the bucket trails the assign.
type FleetCredsRequest struct {
	Executor string `json:"executor"`
	Tenant   string `json:"tenant"`
	Workload string `json:"workload"`
}

// FleetCredsResponse carries the tenant's service creds to the host that
// runs the workload — the only place they rest.
type FleetCredsResponse struct {
	Creds []byte `json:"creds"`
}

// FleetTypeSchemas is the fleet log's vocabulary, provisioned into the
// control account's META so the standard fold judges custody ops like any
// tenant's (0011). All three are merge effects; birth needs no entry — the
// snapshot type is the pattern's own.
func FleetTypeSchemas() map[string]TypeSchema {
	open := json.RawMessage(`{"type":"object"}`)
	m := map[string]TypeSchema{}
	for _, t := range []string{FleetOpAssign, FleetOpRelease, FleetOpStop} {
		m[t] = TypeSchema{Revision: 1, Schema: open, Effect: EffectMerge}
	}
	return m
}

// ValidateWorkloadName checks a workload name is one subject token in the
// identifier grammar.
func ValidateWorkloadName(name string) error {
	if !logName.MatchString(name) {
		return fmt.Errorf("workload name %q: must match [a-z0-9-]+", name)
	}
	return nil
}

// ValidateExecutorName checks an executor ID is one subject token in the
// identifier grammar.
func ValidateExecutorName(name string) error {
	if !logName.MatchString(name) {
		return fmt.Errorf("executor name %q: must match [a-z0-9-]+", name)
	}
	return nil
}
