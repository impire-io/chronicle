# Spec 002 — op types declare their effect

**Work ID:** `0011-op-types-declare-their-effect`
**Design:** `chronicle-hq` @ `22dae92` —
[`03-DECISIONS/0011-op-types-declare-their-effect.md`](../../../chronicle-hq/03-DECISIONS/0011-op-types-declare-their-effect.md)
(the decision),
[`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md)
§ op-type schemas and effects, § the state buckets (the fold's rules),
[`02-DESIGN/04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md)
§ the node's duties (the compaction gate — recorded, not built here);
resolves the build half of tracker item 27.
**Status:** specified — implementation follows on this branch.

## What this delivers

The fold gets its semantics: an op type's META record becomes
`{revision, schema, effect}`, and the node folds declared effects into
`STATE_<LOG>`. `effect: merge` applies the payload to the thing's state
as an RFC 7386 JSON Merge Patch; `effect: none` (the default, and every
unknown value, with a warning) moves nothing. Changing a type's effect
makes the log's derived state suspect: the node purges the state bucket
and re-folds from the start — latest declaration wins. A log whose
types declare nothing behaves exactly as the skeleton built it: no
migration.

## Out of scope

- **Node-driven rollup** and its effect gate — the gate is recorded in
  the fleet design; the triggers are still future work.
- **New effect values** (`set`, `patch`, executable) — additive, by
  demand.
- **Watch-through-rebuild ergonomics** — during a purge-and-refold,
  State() readers may briefly see missing keys; acceptable for the
  skeleton tier.

## Requirements

- **FR-01 Contract.** `TypeSchema` gains `Effect`; `EffectNone`/
  `EffectMerge` constants; `MergePatch` implements RFC 7386 (objects
  merge recursively, `null` deletes, everything else replaces),
  covered by the RFC's own example table.
- **FR-02 Declaring.** `SCHEMA.SET` accepts an effect; empty normalizes
  to `none`; a value outside the node's vocabulary is refused
  (`bad-effect`) — write-side strictness, read-side tolerance. The
  effect rides the same revision as the schema.
- **FR-03 The fold.** Snapshot resets; a valid op of a `merge` type
  applies under the same revision-CAS (stale writers lose, replays
  skip); `none`, unknown types, and unknown effect values move nothing;
  a schema-invalid op of a known type is marked and takes no effect; an
  op on a subject with no snapshot yet is warned about and moves
  nothing.
- **FR-04 Rebuild.** When `SCHEMA.SET` changes a type's effect, the
  node purges the log's state bucket and restarts that log's fold from
  the beginning of the stream; the rebuilt state reflects the new
  declaration over the whole history.
- **FR-05 Surfaces.** `client.SetSchema` carries the effect;
  `chronicle schema set` gains `--effect`.
- **FR-06 The gate.** `make fmt && make test && make lint` green, race
  detector on, every fold behavior proven against a real embedded NATS
  server.
