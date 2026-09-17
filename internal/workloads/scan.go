package workloads

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/impire-io/chronicle/contract"
)

// The level scan (06-scheduler.md § the guard is the arbiter): recovery is
// nobody's job, so it is everybody's. Every tick, every workload in the
// fold is converged — stopped ones torn down, unfilled slots auctioned,
// filled slots cross-checked against the backend witness. Any instance may
// act on anything it sees, because the guard makes eagerness cost one
// rejected append at worst.

func (s *service) scanLoop() {
	ticker := time.NewTicker(s.scanEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		case <-s.kick:
		}
		s.scan()
	}
}

func (s *service) scan() {
	ctx, cancel := context.WithTimeout(s.ctx, s.scanEvery+30*time.Second)
	defer cancel()

	for _, thing := range s.workloadThings() {
		ws, seq, ok := s.workloadState(thing)
		if !ok {
			continue
		}
		if ws.Stopped {
			s.convergeStopped(ctx, thing, ws, seq)
			continue
		}
		s.convergeRunning(ctx, thing, ws, seq)
		if ctx.Err() != nil {
			return
		}
	}
}

// workloadThings snapshots the fold's workload things.
func (s *service) workloadThings() []string {
	var things []string
	for _, thing := range s.pass.Things() {
		if strings.HasPrefix(thing, "workload.") {
			things = append(things, thing)
		}
	}
	return things
}

// convergeStopped tears a retired workload down: destroy at the executor,
// then release the slot with reason stopped. Destroying the absent — and
// asking a gone executor — both count as done.
func (s *service) convergeStopped(ctx context.Context, thing string, ws contract.WorkloadState, seq uint64) {
	name := workloadName(thing, ws)
	for slot, held := range ws.Slots {
		s.destroyPlacement(ctx, held.Executor, ws.Tenant, name)
		if err := s.release(ctx, thing, ws, seq, slot, contract.ReleaseStopped); err != nil && !errors.Is(err, errConflict) {
			s.logger.Warn("workloads: release on stop failed", "thing", thing, "slot", slot, "err", err)
		}
		// One custody write per tick per thing: the guard invalidates our
		// horizon after the first, so the rest wait for the fold.
		return
	}
}

// convergeRunning fills missing slots and cross-checks filled ones.
func (s *service) convergeRunning(ctx context.Context, thing string, ws contract.WorkloadState, seq uint64) {
	name := workloadName(thing, ws)
	if name == "" {
		s.logger.Warn("workloads: unknown kind left alone", "thing", thing, "kind", ws.Kind)
		return
	}

	// Filled slots first: a placement that is gone opens its slot before
	// the auction pass, so the same tick can re-place it next round.
	for slot, held := range ws.Slots {
		status, executorAlive := s.placementStatus(ctx, held.Executor, ws.Tenant, name)
		switch {
		case !executorAlive:
			if err := s.release(ctx, thing, ws, seq, slot, contract.ReleaseLiveness); err != nil && !errors.Is(err, errConflict) {
				s.logger.Warn("workloads: liveness release failed", "thing", thing, "slot", slot, "err", err)
			}
			return // horizon spent; next tick continues
		case status == contract.PlacementNotFound:
			if err := s.release(ctx, thing, ws, seq, slot, contract.ReleaseRestart); err != nil && !errors.Is(err, errConflict) {
				s.logger.Warn("workloads: restart release failed", "thing", thing, "slot", slot, "err", err)
			}
			return
		default:
			// starting or running — both are custody-fine; readiness is the
			// workload's own two-stage story, never the scan's business.
		}
	}

	// Missing slots: auction the gap, one slot per tick per thing — each
	// assign spends the horizon.
	for i := 0; i < ws.Replicas; i++ {
		slot := strconv.Itoa(i)
		if _, filled := ws.Slots[slot]; filled {
			continue
		}
		key := thing + "#" + slot
		s.mu.Lock()
		_, inflight := s.auctioning[key]
		if !inflight {
			s.auctioning[key] = struct{}{}
		}
		s.mu.Unlock()
		if inflight {
			continue
		}
		s.auctionSlot(ctx, thing, ws, seq, slot)
		s.mu.Lock()
		delete(s.auctioning, key)
		s.mu.Unlock()
		return
	}
}

// placementStatus asks the backend witness. The second return is the
// executor witness: false means nobody answered — the executor is gone,
// which is a different reason on the record than a live executor that lost
// the placement.
func (s *service) placementStatus(ctx context.Context, executor, tenant, workload string) (string, bool) {
	reqData, err := json.Marshal(contract.FleetStatusRequest{Tenant: tenant, Workload: workload})
	if err != nil {
		return "", false
	}
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	msg, err := s.nc.RequestWithContext(reqCtx, contract.FleetStatusSubject(executor), reqData)
	if err != nil {
		return "", false
	}
	var resp contract.FleetStatusResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return "", false
	}
	if resp.Status == "" {
		return contract.PlacementNotFound, true
	}
	return resp.Status, true
}
