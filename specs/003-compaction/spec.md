# Spec 003 — compaction

**Work ID:** `28` (tracker item 28)
**Design:** `chronicle-hq` @ `57e3674` —
[`02-DESIGN/04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md)
§ the node's duties (rollups),
[`03-DECISIONS/0011-op-types-declare-their-effect.md`](../../../chronicle-hq/03-DECISIONS/0011-op-types-declare-their-effect.md)
§ 4 (effects gate compaction), and the
[ops-log pattern](../../../chronicle-hq/99-ARTIFACTS/ops-log-pattern.md)
§ 5.2–5.4 (rollup mechanics).
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

The compaction path, both halves. Nothing in the system ever set
`Nats-Rollup`; every log stream carries `MaxBytes` + `DiscardNew`, so a
busy log filled its budget with no way to shrink history but raising it.

- **The SDK's "save version"**: `client.SaveVersion` publishes an
  app-materialised snapshot that replaces the thing's history in one
  write — `Nats-Rollup: sub` plus the expected-sequence guard (pattern
  § 5.2). The app supplies the state (folded with its own semantics —
  this is the app's call, so a tail holding `effect: none` ops may be
  compacted here and only here), the frontier, and `upTo`, the stream
  seq of the last op the state covers. If anything landed after `upTo`
  the server refuses and nothing changes: `ErrStaleVersion` — re-read,
  re-fold, retry. A retry of a save that already landed reports success
  (same recovery as `CreateThing`: the guard fires before dedup).
- **The node's triggers**: a timer over active subjects
  (`Config.RollupEvery`, default 1 h) and the on-demand
  `CHRON.API.THING.ROLLUP` verb. Both run the same routine: replay the
  exact subject, fold it under the current declarations (0011), and
  publish the result as a rollup snapshot guarded by the last replayed
  seq — race-safe against anyone else's rollup, including an SDK save
  (first writer wins, the loser discards).

## The effect gate, as built

Decision 0011 § 4: the node compacts only subjects whose tail is fully
effect-covered. Two readings were possible; this build takes the
strictest, because rollup destroys **every** earlier message on the
subject (§ 5.2), not just the tail since the last snapshot:

> The node compacts only history its fold fully captured into state:
> every message the rollup would destroy must be a snapshot or a
> schema-valid op of a known type whose declared effect is known and
> not `none`.

So any of the following anywhere in the subject's remaining history
vetoes the node's rollup and leaves compaction the application's call
(`SaveVersion`): an `effect: none` op, an unknown op type, an unknown
effect value, a schema-invalid op of a known type (marked, never
dropped — destroying it silently would unsay the mark), and any op
before the first snapshot. A veto is an answer, not an error: the verb
reports `rolled: false` with the reason.

## Out of scope

- **Archiving before compaction** (§ 5.6 sourcing) and op signing —
  the one-way-door checklist stays the operator's until a design
  demands rails.
- **A log-wide or account-wide rollup verb** — rollup is per exact
  subject (§ 5.4); the timer already sweeps active subjects.
- **SDK-computed generic rollup** (bucket state + generic tail fold) —
  the node's triggers cover the generic case; an app that calls
  `SaveVersion` brings its own fold by definition.
- **Terminal-status triggers** (pattern § 5.2 "always on a terminal
  status") — no lifecycle vocabulary exists yet to hang it on.

## Requirements

- **FR-01 SDK.** `SaveVersion(ctx, log, thing, state, frontier, upTo,
  opts...)` publishes the snapshot with `Nats-Rollup: sub` and
  `Nats-Expected-Last-Subject-Sequence: upTo`; refuses `upTo == 0`
  (birth is `CreateThing`); maps wrong-last-sequence to
  `ErrStaleVersion` unless the subject's last op is this very save
  (retry-safe); holds the per-subject in-flight lock.
- **FR-02 The routine.** The node replays the exact subject, judges
  every message under the gate above, folds snapshot-resets and merge
  patches in memory, and publishes `{state, frontier}` with the guard
  set to the last replayed seq. Losing the race is a skip, not an
  error. No snapshot yet, or a single-message history, is a skip
  ("nothing to compact").
- **FR-03 The verb.** `CHRON.API.THING.ROLLUP`
  (`{principal, log, thing}` → `{rolled, seq, reason}`), admin or
  writer (readers are refused); unknown log and unknown thing are
  named errors. `client.RollupThing` speaks it; `chronicle thing
  rollup <log> <thing>` surfaces it.
- **FR-04 The timer.** The fold records each ops-family subject it
  consumes; every `RollupEvery` (default 1 h, zero means the default)
  the node swaps each log's active set and runs the routine per
  subject. A subject re-enters the set on its next op. After a node
  restart or an effect-change rebuild the re-fold marks everything
  active, so the next tick sweeps the whole log — steady-state
  hygiene, not a bug.
- **FR-05 Serialization.** Rollups and the effect-change rebuild
  (`rebuildLog`) exclude each other per log, so a rollup never
  publishes a snapshot computed under declarations mid-change.
- **FR-06 The gate.** `make fmt && make test && make lint` green, race
  detector on, every behavior proven against a real embedded NATS
  server — including: SaveVersion compacts and survives its own retry;
  a stale save loses and the log is untouched; the verb compacts a
  covered history; `none`, unknown-type, and marked ops each veto;
  the timer compacts a covered subject and leaves a vetoed one alone.
