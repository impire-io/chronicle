# Spec 005 — fleet scheduling: the loop and backend zero

**Work ID:** `chronicle-1` (tracker item chronicle-1)
**Design:** `chronicle-hq` @ `36e9677` —
[`02-DESIGN/06-scheduler.md`](../../../chronicle-hq/02-DESIGN/06-scheduler.md)
(the fleet log, the dispatch surface, the auction, the guard, the executors,
the workload contract, replicas and supervision) and
[`03-DECISIONS/0014-the-fleet-runs-on-chronicle.md`](../../../chronicle-hq/03-DECISIONS/0014-the-fleet-runs-on-chronicle.md)
(superseding 0013).
**Status:** in progress on this branch ([plan.md](plan.md)).

## What this delivers

Decision 0014's core loop, end to end on the in-process backend: workload
scheduling as a chronicle log, placed by auction, arbitrated by the guard.
Item 29's scheduler-less META watcher is absorbed; nothing schedules by
spawning goroutines from a KV watch anymore.

- **The fleet log**: log `fleet` in the control account — stream
  `LOG_FLEET` capturing `CHRON.fleet.>`, state bucket `STATE_FLEET`, both
  provisioned by control at bootstrap alongside the control account's own
  `META` carrying the vocabulary's type schemas. Two thing families:
  `CHRON.fleet.OPS.executor.<executor>` and
  `CHRON.fleet.OPS.workload.<tenant>.<workload>`.
- **The vocabulary**: birth is the pattern's own `snapshot` type — §5.1,
  guard at sequence 0; "dispatch" and "register" are the API verbs whose
  record is that snapshot. Custody moves by merge types
  `workload.assign`, `workload.release`, `workload.stop`, each landing
  under `Nats-Expected-Last-Subject-Sequence` stamped with the writer's
  knowledge horizon — a competing write is rejected by the server, and
  the loser re-reads. Failure counters ride the merged state
  (`failures`), never erasable history. `executor.update/drain/deregister`
  wait for their consumers (the multi-host increment).
- **`chronicle-workloads`**: a micro service in the control account — the
  fleet log's **only writer**. Serves `CHRON.API.FLEET.DISPATCH` and
  `.STOP` (queue-grouped), folds the fleet log through the shared judge
  into `STATE_FLEET` (revision-CAS), runs the auctions, and level-scans:
  unfilled slots are auctioned, assignments are cross-checked against the
  two witnesses (executor micro liveness, backend `status`), and a
  placement that is gone is released with its reason and re-auctioned.
- **The auction**: scatter on `CHRON.API.FLEET.AUCTION` (a plain
  subscription in every executor — deliberately not a queue group),
  live-capacity bids gathered in a bounded window, delegation by direct
  request to the winner, **accept before the op lands**. Zero bids read
  as unschedulable, surfaced and retried — never as "no fleet".
- **`chronicle-executor`**: registers itself (snapshot on its roster
  subject, written for it by the service), bids, runs delegations through
  the `ensure/status/destroy` actuator seam, restarts its own placements
  locally with backoff (no log traffic), and reports custody events to
  `CHRON.API.FLEET.REPORT` when a restart budget is exhausted. **Backend
  zero** is the in-process actuator: kind `node` runs `node.Start`, kind
  `index-search` runs `search.Start`, each on a connection holding that
  tenant's service user only.
- **Credentials are a record-verified pull**: `CHRON.API.FLEET.CREDS` on
  control — the executor asks for an assigned workload's service creds;
  control verifies the assignment against `STATE_FLEET`, falling back to
  folding the workload's subject from the log's tail when the bucket
  trails, and only then returns the tenant's service user. The record is
  the authorization; no issuance ACL.
- **The node reports its slice**: `INDEX.DECLARE`/`INDEX.DELETE` report
  dispatch/stop for `index-search` workloads from the same handler that
  writes the META key, and the node re-derives its slice from META at
  boot — the level-triggered healing of the design. In this in-process
  composition the report is wired directly by the composition root; the
  tenant-stamped account import lands with the multi-host increment.
- **`chronicle up`** composes: bootstrap NATS, control, one
  `chronicle-workloads` instance, one embedded executor (backend zero).
  Control dispatches a `node` workload per tenant — at mint and for each
  tenant on disk at boot (idempotent: birth's zero guard makes the second
  dispatch a no-op). On restart, `STATE_FLEET` persists while in-process
  placements do not: the level scan's backend-status witness reports them
  `not-found`, releases, and re-auctions — recovery exercised on every
  reboot.

## Out of scope

Per the design's "what this does not do" and the minimal posture: the
**microsandbox backend** (the first 0004 backend — its own item, next,
now that `msb` is present on the dev host; backend zero is the loop's
precondition, not a 0004 backend); docker and kubernetes; the
tenant-stamped account import for the node's report (multi-host);
`executor.drain`/`update`/`deregister` flows; scale-to-zero; placement
policy beyond constraints; `workload.scale`; exec/log streaming; fleet
rollup/compaction tuning; the static control node (control's records are
still files — their migration to control-account ops-logs is a separate
future handoff); tenant teardown (mint has no delete verb yet).
