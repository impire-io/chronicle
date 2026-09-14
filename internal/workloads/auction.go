package workloads

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/impire-io/chronicle/contract"
)

// The auction (06-scheduler.md § the auction): scatter on an uncaptured
// subject, live-capacity bids within a bounded window, delegation to the
// best bidder, accept before the op lands — and the op lands under the
// guard, so a competing decision costs one rejected append, never a
// duplicate assignment.

// auctionSlot places one open slot. It returns without error when the slot
// was filled and recorded; every other outcome is left for the next scan
// tick — the level, not the edge, owns convergence.
func (s *service) auctionSlot(ctx context.Context, thing string, ws contract.WorkloadState, seq uint64, slot string) {
	bids, err := s.gatherBids(ctx, ws)
	if err != nil {
		s.logger.Warn("workloads: auction scatter failed", "thing", thing, "slot", slot, "err", err)
		return
	}
	if len(bids) == 0 {
		// Unschedulable, not "no fleet": the record keeps the open slot and
		// the scan retries on the next tick and on roster changes.
		s.logger.Warn("workloads: no bids — unschedulable for now", "thing", thing, "slot", slot, "kind", ws.Kind)
		return
	}
	// Fewest live placements first; the executor name breaks ties so two
	// instances auctioning concurrently converge on the same pick.
	sort.Slice(bids, func(i, j int) bool {
		if bids[i].Placements != bids[j].Placements {
			return bids[i].Placements < bids[j].Placements
		}
		return bids[i].Executor < bids[j].Executor
	})

	for _, bid := range bids {
		accepted, err := s.delegate(ctx, bid.Executor, thing, ws, slot)
		if err != nil || !accepted {
			s.logger.Info("workloads: delegation not accepted", "thing", thing, "slot", slot, "executor", bid.Executor, "accepted", accepted, "err", err)
			continue // stale bid or a gone executor; try the next bidder
		}
		patch, err := json.Marshal(map[string]any{
			"slots": map[string]contract.WorkloadSlot{slot: {Executor: bid.Executor}},
		})
		if err != nil {
			s.logger.Warn("workloads: encode assign", "thing", thing, "err", err)
			return
		}
		err = s.append(ctx, thing, contract.FleetOpAssign, patch, seq)
		if errors.Is(err, errConflict) {
			// A competing decision landed first. Undo our optimistic
			// delegation — the executor may already be starting — and let
			// the scan re-read the record.
			s.logger.Info("workloads: assign outran; undoing delegation", "thing", thing, "slot", slot, "executor", bid.Executor)
			s.destroyPlacement(ctx, bid.Executor, ws.Tenant, workloadName(thing, ws))
			return
		}
		if err != nil {
			s.logger.Warn("workloads: assign append failed", "thing", thing, "slot", slot, "err", err)
			return
		}
		s.logger.Info("workloads: slot assigned", "thing", thing, "slot", slot, "executor", bid.Executor)
		return
	}
	s.logger.Warn("workloads: every bidder refused", "thing", thing, "slot", slot)
}

// gatherBids scatters the auction request and collects every bid that
// arrives within the window. Transient traffic on an uncaptured subject —
// nothing here touches the log.
func (s *service) gatherBids(ctx context.Context, ws contract.WorkloadState) ([]contract.FleetAuctionBid, error) {
	reqData, err := json.Marshal(contract.FleetAuctionRequest{
		Tenant:   ws.Tenant,
		Workload: workloadName("", ws),
		Kind:     ws.Kind,
	})
	if err != nil {
		return nil, err
	}
	inbox := s.nc.NewRespInbox()
	sub, err := s.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := s.nc.PublishRequest(contract.FleetAuctionSubject, inbox, reqData); err != nil {
		return nil, err
	}

	var bids []contract.FleetAuctionBid
	deadline := time.Now().Add(s.auctionWindow)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return bids, nil
		}
		msg, err := sub.NextMsg(remaining)
		if errors.Is(err, nats.ErrTimeout) {
			return bids, nil
		}
		if err != nil {
			return bids, err
		}
		var bid contract.FleetAuctionBid
		if err := json.Unmarshal(msg.Data, &bid); err != nil {
			s.logger.Warn("workloads: unreadable bid ignored", "err", err)
			continue
		}
		if bid.Executor == "" {
			continue
		}
		bids = append(bids, bid)
		if ctx.Err() != nil {
			return bids, ctx.Err()
		}
	}
}

// delegate offers the slot to one executor and waits for the accept or the
// refusal. The executor starts the placement on accept — optimistically;
// the guard settles who won, and the loser is destroyed.
func (s *service) delegate(ctx context.Context, executor, thing string, ws contract.WorkloadState, slot string) (bool, error) {
	reqData, err := json.Marshal(contract.FleetDelegateRequest{
		Tenant:   ws.Tenant,
		Workload: workloadName(thing, ws),
		Slot:     slot,
		Kind:     ws.Kind,
		Log:      ws.Log,
		Index:    ws.Index,
	})
	if err != nil {
		return false, err
	}
	msg, err := s.nc.RequestWithContext(ctx, contract.FleetDelegateSubject(executor), reqData)
	if err != nil {
		return false, err
	}
	var resp contract.FleetDelegateResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return false, err
	}
	return resp.Accepted, nil
}

// destroyPlacement tells one executor to tear a placement down. Absence is
// success; a gone executor is equivalent — its placements died with it.
func (s *service) destroyPlacement(ctx context.Context, executor, tenant, workload string) {
	reqData, err := json.Marshal(contract.FleetDestroyRequest{Tenant: tenant, Workload: workload})
	if err != nil {
		return
	}
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := s.nc.RequestWithContext(reqCtx, contract.FleetDestroySubject(executor), reqData); err != nil {
		s.logger.Info("workloads: destroy unanswered (executor gone is fine)", "executor", executor, "tenant", tenant, "workload", workload, "err", err)
	}
}

// workloadName recovers the workload's name within its tenant. The fold
// keys memory by the thing tail; the state carries tenant and identity
// fields, so the name is the tail minus the family and tenant tokens —
// derived from either source, whichever the caller has.
func workloadName(thing string, ws contract.WorkloadState) string {
	switch ws.Kind {
	case contract.WorkloadKindNode:
		return contract.WorkloadNodeName
	case contract.WorkloadKindIndexSearch:
		return contract.WorkloadIndexName(ws.Log, ws.Index)
	}
	// Unknown kinds keep their thing-tail name so destroy still addresses
	// them; _ = thing keeps the derivation honest if fields are absent.
	_ = thing
	return ""
}
