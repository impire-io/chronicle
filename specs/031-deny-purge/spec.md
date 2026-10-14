# Spec 031 — a full-history store refuses purge

**Work ID:** `chronicle-70` (tracker item chronicle-70, a bug)
**Design:** `chronicle-hq` @ `754613f` (hq PR #87) —
[`03-DECISIONS/0019-a-log-may-declare-its-history-preserved.md`](../../../chronicle-hq/03-DECISIONS/0019-a-log-may-declare-its-history-preserved.md)
§ amendment of 2026-10-14, and the amended designs
[`02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md)
§ streams (the `DenyPurge` row),
[`03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md)
§ store configuration and
[`04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md) § rollups.
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

The missing half of spec 011's server-side guarantee. Decision 0019 said
a preserved store's stream refuses every sanctioned way history shrinks,
on the strength of `AllowRollup: false` and `DenyDelete: true`. It does
not: `DenyDelete` refuses deleting a single message by sequence only, and
a purge — of one subject's history or of the whole stream — is a separate
JetStream API call that `DenyPurge` governs, which nothing here ever set.
Measured, a `history: full` store's stream accepted both purges.

- **The stream.** `StoreStreamConfig` sets `DenyPurge` exactly when the
  store's history is `full` — beside `AllowRollup: false` and
  `DenyDelete: true`. Every other row of the table stands, and every
  other store keeps `DenyPurge` off: the server refuses `DenyPurge`
  beside `AllowRollup`, so the setting belongs to preserved stores only.
- **Nothing else moves.** The node never purges a store's stream (it
  purges STATE buckets on a declaration change, a different resource);
  a member cannot reach `$JS.API`; account destruction deletes streams.
  The SDK artifact describes no stream setting, so the contract version
  stands.

## Out of scope

- **Streams that already exist.** The node only creates streams. A
  preserved stream created before this lands takes `DenyPurge` by a
  one-off operator step (a stream configuration update may turn it on,
  never off); the beta stores are expected to be recreated, so there is
  no migration code and no runbook entry (tracker chronicle-70).
- **`DenyPurge` on compactable stores.** The server refuses it beside
  rollup, and rollup is what a compactable store is for.

## Requirements

- **FR-01 The stream.** `StoreStreamConfig(store, budget, HistoryFull,
  schedules)` carries `DenyPurge: true`; with `HistoryCompactable` or
  the empty declaration it carries `DenyPurge: false`.
- **FR-02 The refusal is the server's.** On a real NATS server, a purge
  of a full-history store's subjects and a purge of its whole stream are
  both refused by the server (error 10110, "stream purge not permitted"),
  and the trail is intact afterwards.
- **FR-03 The gate (quality).** `make check` green, race on.
