# Spec 001 — the walking skeleton

**Work ID:** `0010-chronicle-repo`
**Design:** `chronicle-hq` @ `84f77ed` —
[`03-DECISIONS/0010-chronicle-repo.md`](../../../chronicle-hq/03-DECISIONS/0010-chronicle-repo.md)
(this build's mandate),
[`03-DECISIONS/0008-tenant-data-plane-contract.md`](../../../chronicle-hq/03-DECISIONS/0008-tenant-data-plane-contract.md)
(the contract), carried by the four designs in reading order:
[`02-DESIGN/01-onboarding.md`](../../../chronicle-hq/02-DESIGN/01-onboarding.md),
[`02-DESIGN/02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md),
[`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md),
[`02-DESIGN/04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md).
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

One tenant end to end, the fleet design's walking-skeleton floor: mint →
node running for the account → create a log → publish ops with pre-flight
validation → fold → state bucket → read current state and replay history.
For local development, `chronicle up` carries control, a scheduler-less
node, and the bootstrap NATS in one process — the fleet shape without the
fleet ceremony.

The client surface of the skeleton is the in-repo Go client package.
Whether it becomes the published Go SDK, and where the other SDKs live,
stays open per 0010 — settled when the first SDK is built, not here.

## Out of scope

- **The first index kind (search)** — follows once the spine holds (fleet
  design); no `chronicle-index-*` workload in the skeleton.
- **`chronicle-scheduler` and its backend plugins** — the skeleton runs
  scheduler-less (`chronicle up`); placement and supervision stay decision
  0004's own design, picked up when a second workload shape exists.
- **Rollup triggers** — optional for correctness underneath by design;
  the spine must hold without a roller before one is added.
- **Scale-to-zero** — named and deferred in the fleet design.
- **Per-role wire permissions and op signing** — the deferred-by-demand
  hardenings on tracker items 23 and 24.
- **The web control panel and non-Go SDKs** — surfaces of decision 0005,
  after the spine.

## Requirements

- **FR-01 Mint.** `chronicle-control` performs the onboarding design's
  mint: tenant account, the member-baseline scoped key granting `CHRON.>`
  once at account creation, the `META` bucket provisioned, the identity
  registry seeded. Deliverable: credentials a client connects with.
- **FR-02 Create log.** A `CHRON.API.>` verb on the node creates stream
  `LOG_<LOG>` with the wire contract's settings (limits retention, rollup
  allowed, delete denied, dedup window, byte budget with `DiscardNew`)
  plus the META entries — reserved log names refused, no key operations,
  no JWT pushes.
- **FR-03 Append.** The Go client publishes ops directly to
  `CHRON.<log>.OPS.<thing…>` carrying the operation record (`Nats-Msg-Id`,
  `Op-Type`, `Op-Author`, `Op-Parents`, `Op-Ts`, `Op-Version: 1`), with
  pre-flight validation against the log's META schemas, one in-flight
  publish per subject, and birth via the expected-sequence-zero guard.
  Nothing sits between the writer and the log.
- **FR-04 Fold.** `chronicle-node` runs an ordered consumer per log, one
  fold state per subject, and maintains `STATE_<LOG>` (`{seq, state}`)
  under revision-CAS. Unknown op types are warned about; schema-invalid
  payloads are marked, never dropped.
- **FR-05 Read.** Current state is a KV read of `STATE_<LOG>`; a reader
  that must be exact reads the value then folds the log from `seq + 1`;
  history is a replay of `LOG_<LOG>`.
- **FR-06 `chronicle up`.** One process: control, a scheduler-less node,
  and the bootstrap NATS — a developer goes from nothing to FR-01→FR-05
  on one machine with one command.
- **FR-07 The gate.** `make fmt && make test && make lint` green with the
  race detector; every wire-contract behavior tested against an embedded
  NATS server, no mocked NATS client anywhere.
