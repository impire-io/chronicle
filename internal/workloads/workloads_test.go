package workloads_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/impire-io/chronicle/contract"
	"github.com/impire-io/chronicle/internal/natstest"
	"github.com/impire-io/chronicle/internal/workloads"
)

// fakeExecutor answers the executor's wire surface from a test: bids on
// every auction, accepts every delegation, and reports the status the test
// sets — the muscle reduced to its protocol.
type fakeExecutor struct {
	t      *testing.T
	nc     *nats.Conn
	id     string
	status atomic.Value // string

	destroys atomic.Int64
}

func startFakeExecutor(t *testing.T, nc *nats.Conn, id string) *fakeExecutor {
	t.Helper()
	f := &fakeExecutor{t: t, nc: nc, id: id}
	f.status.Store(contract.PlacementRunning)

	subs := []struct {
		subject string
		handler nats.MsgHandler
	}{
		{contract.FleetAuctionSubject, func(msg *nats.Msg) {
			bid, _ := json.Marshal(contract.FleetAuctionBid{Executor: id, Placements: 0})
			_ = msg.Respond(bid)
		}},
		{contract.FleetDelegateSubject(id), func(msg *nats.Msg) {
			resp, _ := json.Marshal(contract.FleetDelegateResponse{Accepted: true})
			_ = msg.Respond(resp)
		}},
		{contract.FleetStatusSubject(id), func(msg *nats.Msg) {
			resp, _ := json.Marshal(contract.FleetStatusResponse{Status: f.status.Load().(string)})
			_ = msg.Respond(resp)
		}},
		{contract.FleetDestroySubject(id), func(msg *nats.Msg) {
			f.destroys.Add(1)
			resp, _ := json.Marshal(contract.FleetDestroyResponse{Destroyed: true})
			_ = msg.Respond(resp)
		}},
	}
	for _, s := range subs {
		sub, err := nc.Subscribe(s.subject, s.handler)
		if err != nil {
			t.Fatalf("subscribe %s: %v", s.subject, err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return f
}

func startService(ctx context.Context, t *testing.T) (*nats.Conn, *workloads.Service) {
	t.Helper()
	url := natstest.StartJetStream(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	svc, err := workloads.Start(ctx, nc, workloads.Config{
		ScanEvery:     150 * time.Millisecond,
		AuctionWindow: 80 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start workloads: %v", err)
	}
	t.Cleanup(svc.Stop)
	return nc, svc
}

func workloadFromBucket(ctx context.Context, t *testing.T, nc *nats.Conn, tenant, workload string) (contract.WorkloadState, bool) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	states, err := js.KeyValue(ctx, contract.StateBucket(contract.FleetLog))
	if err != nil {
		t.Fatalf("open STATE_FLEET: %v", err)
	}
	entry, err := states.Get(ctx, contract.FleetWorkloadThing(tenant, workload))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return contract.WorkloadState{}, false
	}
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var sv contract.StateValue
	if err := json.Unmarshal(entry.Value(), &sv); err != nil {
		t.Fatalf("decode state value: %v", err)
	}
	var ws contract.WorkloadState
	if err := json.Unmarshal(sv.State, &ws); err != nil {
		t.Fatalf("decode workload state: %v", err)
	}
	return ws, true
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never observed: %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestAuctionPlacesReportReleasesAndStopRetires drives the custody
// lifecycle over the wire: dispatch → auction → assign lands in
// STATE_FLEET; a failing report opens the slot, bumps the counter in
// merged state, and the level scan re-places it; stop destroys and
// releases with reason stopped.
func TestAuctionPlacesReportReleasesAndStopRetires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nc, _ := startService(ctx, t)
	fake := startFakeExecutor(t, nc, "fake")

	if _, err := workloads.Register(ctx, nc, contract.FleetRegisterRequest{Executor: "fake", Backend: "test"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := workloads.Dispatch(ctx, nc, contract.FleetDispatchRequest{
		Tenant: "t1", Workload: contract.WorkloadNodeName, Kind: contract.WorkloadKindNode,
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	waitFor(t, "slot assigned to fake", func() bool {
		ws, ok := workloadFromBucket(ctx, t, nc, "t1", contract.WorkloadNodeName)
		return ok && ws.Slots["0"].Executor == "fake"
	})

	// Dispatching again is the idempotent no-op, not a second birth.
	resp, err := workloads.Dispatch(ctx, nc, contract.FleetDispatchRequest{
		Tenant: "t1", Workload: contract.WorkloadNodeName, Kind: contract.WorkloadKindNode,
	})
	if err != nil || !resp.Existed {
		t.Fatalf("re-dispatch: err=%v existed=%v", err, resp.Existed)
	}

	// The budget-exhausted report: the slot opens with the failure on the
	// merged state, then the scan re-places it.
	if _, err := workloads.Report(ctx, nc, contract.FleetReportRequest{
		Executor: "fake", Tenant: "t1", Workload: contract.WorkloadNodeName, Slot: "0", Reason: contract.ReleaseFailing,
	}); err != nil {
		t.Fatalf("report: %v", err)
	}
	waitFor(t, "failure counted and slot re-assigned", func() bool {
		ws, ok := workloadFromBucket(ctx, t, nc, "t1", contract.WorkloadNodeName)
		return ok && ws.Failures != nil && ws.Failures.Count == 1 && ws.Slots["0"].Executor == "fake"
	})

	// Retirement: destroy reaches the executor, the slot releases stopped.
	if _, err := workloads.StopWorkload(ctx, nc, contract.FleetStopRequest{Tenant: "t1", Workload: contract.WorkloadNodeName}); err != nil {
		t.Fatalf("stop: %v", err)
	}
	waitFor(t, "workload retired", func() bool {
		ws, ok := workloadFromBucket(ctx, t, nc, "t1", contract.WorkloadNodeName)
		return ok && ws.Stopped && len(ws.Slots) == 0
	})
	if fake.destroys.Load() == 0 {
		t.Fatal("stop never reached the executor")
	}
}

// TestZeroBidsIsUnschedulableNotFatal: a dispatch nobody can carry keeps
// its open slot on the record and is placed the moment a bidder appears —
// unschedulable, never "no fleet".
func TestZeroBidsIsUnschedulableNotFatal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nc, _ := startService(ctx, t)

	if _, err := workloads.Dispatch(ctx, nc, contract.FleetDispatchRequest{
		Tenant: "t1", Workload: contract.WorkloadNodeName, Kind: contract.WorkloadKindNode,
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	// Let a few scan ticks pass with no fleet at all.
	time.Sleep(500 * time.Millisecond)
	ws, ok := workloadFromBucket(ctx, t, nc, "t1", contract.WorkloadNodeName)
	if !ok || len(ws.Slots) != 0 || ws.Stopped {
		t.Fatalf("unschedulable workload should keep its open record: ok=%v %+v", ok, ws)
	}

	startFakeExecutor(t, nc, "late")
	if _, err := workloads.Register(ctx, nc, contract.FleetRegisterRequest{Executor: "late", Backend: "test"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	waitFor(t, "late bidder placed", func() bool {
		ws, ok := workloadFromBucket(ctx, t, nc, "t1", contract.WorkloadNodeName)
		return ok && ws.Slots["0"].Executor == "late"
	})
}

// TestGuardRejectsTheOutrunWriter is the arbitration mechanism itself:
// birth's zero guard settles racing dispatches, and a custody write whose
// knowledge horizon the log has outrun is rejected by the server — clean
// history, no election, no duplicate assignments.
func TestGuardRejectsTheOutrunWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nc, _ := startService(ctx, t) // provisions LOG_FLEET

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	subject := contract.OpsSubject(contract.FleetLog, contract.FleetWorkloadThing("t9", "node"))
	publish := func(opType string, payload string, expected uint64) error {
		op := contract.Op{ID: nats.NewInbox(), Type: opType, Author: "test"}
		msg := &nats.Msg{Subject: subject, Header: op.Header(), Data: []byte(payload)}
		_, err := js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequencePerSubject(expected))
		return err
	}

	if err := publish(contract.OpTypeSnapshot, `{"state":{"kind":"node","tenant":"t9","replicas":1,"slots":{}}}`, 0); err != nil {
		t.Fatalf("birth: %v", err)
	}
	// A second birth loses the race at the server.
	if err := publish(contract.OpTypeSnapshot, `{"state":{}}`, 0); err == nil {
		t.Fatal("second birth landed; the zero guard did not hold")
	}

	md, err := js.Stream(ctx, contract.StreamName(contract.FleetLog))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := md.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	seq := info.State.LastSeq

	// A write stamped with the current horizon lands …
	if err := publish(contract.FleetOpAssign, `{"slots":{"0":{"executor":"a"}}}`, seq); err != nil {
		t.Fatalf("assign at the horizon: %v", err)
	}
	// … and the competing write stamped with the same, now-outrun horizon
	// is rejected: one winner, decided by the server, visible to the loser.
	err = publish(contract.FleetOpAssign, `{"slots":{"0":{"executor":"b"}}}`, seq)
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
		t.Fatalf("competing assign: want wrong-last-sequence, got %v", err)
	}
}
