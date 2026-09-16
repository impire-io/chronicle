# Spec 011 — preserved history

**Work ID:** `chronicle-11` (tracker item chronicle-11)
**Design:** `chronicle-hq` @ `4fd84ab` —
[`03-DECISIONS/0019-a-log-may-declare-its-history-preserved.md`](../../../chronicle-hq/03-DECISIONS/0019-a-log-may-declare-its-history-preserved.md),
[`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md)
§ log configuration, [`02-DESIGN/04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md)
§ the node's duties (rollups), and
[`02-DESIGN/02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md)
§ streams (`AllowRollup` follows the declaration).
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

The per-log half of the compaction story. Decision 0011 § 4 protects
history per message — an effect-none op in a tail vetoes the node's
rollup — but a log whose trail *is* the product had no way to say so:
adopting merge effects for folded state made every covered thing
sweep-eligible, and the only refuge (effect-none everywhere) forfeits
state entirely.

- **`log.<log>.config` carries `history`**: `compactable` (the default —
  today's behavior, no migration) or `preserved`. Declared at `LOG.CREATE`,
  write-side strict, immutable for now — no config-edit verb exists, and
  adding one is its own future decision.
- **The node never compacts a preserved log.** The gate sits at the top
  of the one routine both triggers share, so the timer sweep skips it
  and the on-demand verb declines it with the declaration named.
- **The guarantee is the server's**: a preserved log's stream is created
  with `AllowRollup: false`, so no writer — node, app (`SaveVersion`),
  or accident — can replace a subject's history. `DenyDelete` already
  held. Effects keep their first duty untouched: merge ops still fold
  into state buckets and indexes.

## Out of scope

- **Flipping the declaration.** Editing log config needs a verb that
  does not exist; until that decision, `history` stands for the log's
  life.
- **Budget arithmetic.** A preserved log still meets `MaxBytes` +
  `DiscardNew` head-on; raising the budget stays the tenant's deliberate
  act.
- **The fleet log.** It embraces compaction (0014) and stays
  compactable.

## Requirements

- **FR-01 The declaration.** `LogCreateRequest.History` — empty,
  `compactable`, or `preserved`; anything else is refused (`bad-history`,
  write-side strict). Recorded in `LogConfig` beside status and budget;
  `contract.NormalizeHistory` maps unset to compactable, and an unknown
  stored value reads as compactable (read-side tolerance — the stream
  setting is the guarantee either way).
- **FR-02 The stream.** `LogStreamConfig` takes the declaration:
  `AllowRollup` is false exactly when the log is preserved; every other
  row of the 0008 table stands.
- **FR-03 The gate.** `rollupThing` declines a preserved log before
  replaying anything, reason naming the declaration — covering the timer
  and the verb in one place.
- **FR-04 The surface.** `client.CreateLog` grows variadic options with
  `WithHistory`; `chronicle log create` takes `--history`.
- **FR-05 The gate (quality).** `make check` green, race on, proven
  against a real embedded NATS server — including: the unknown value
  refused; the verb declining a fully merge-covered thing on a preserved
  log with the reason named; `SaveVersion` refused by the stream itself;
  the trail intact afterwards; the timer compacting a compactable
  control log while the preserved log of the same shape survives the
  same sweeps.
