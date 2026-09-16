# Spec 012 — ops-sourced indexes

**Work ID:** `chronicle-12` (tracker item chronicle-12)
**Design:** `chronicle-hq` @ `d05c7af` —
[`03-DECISIONS/0020-an-index-may-source-from-ops.md`](../../../chronicle-hq/03-DECISIONS/0020-an-index-may-source-from-ops.md)
and [`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md)
§ the declaration (`source`), § what an index materializes, and the
per-kind sections.
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

An index may now read the log's other face. Every kind materialized
folded thing state only, so an `effect: none` op's content — a report
body, a trail note — was invisible to every index, and a log whose
meaning lives in its history could not make that history findable.

- **`source: state | ops` in the declaration config**, orthogonal to
  kind, default `state` — today's behavior, no migration. Search and
  semantic support `ops`; graph refuses it write-side (state-only by
  0015's purity rule, its reversal a future decision). `types` narrows
  which op types an ops-sourced index reads.
- **An ops-sourced pass reads history as it is**: every op, unknown
  types included, no judge, no snapshot gate — schemas and effects shape
  state, never history's visibility. It never rebuilds on an effect
  change, and ops being immutable, each document is made (and for
  semantic, embedded) exactly once.
- **Hits stay thing-level `{thing, score}`**, scored by the best op —
  the reply contract and the authority rule unchanged; internally a
  document is keyed by thing and stream seq.
- **Search now reads its declaration at start** — until now it never
  needed to; its config admits exactly `{source, types}` (the no-knobs
  rule stands: source names what is read, never how it is analyzed).

## Out of scope

- **A per-op query surface** — an ops-sourced hit still names a thing;
  a caller that wants the matching op replays the thing.
- **Graph from ops** — refused at DECLARE with the reason; its
  retraction story belongs to a future decision with a real consumer.
- **Persisted indexes** — boot stays a replay (and a re-embed).

## Requirements

- **FR-01 The config.** `ParseSearchConfig` (new) and
  `ParseSemanticConfig` admit `source` (vocabulary `state|ops`,
  write-side strict) and `types` (ops-source only, non-empty, no
  duplicates); `ParseGraphConfig` admits `source: state` and refuses
  `ops` naming the rule. DECLARE validates search config instead of
  refusing all of it.
- **FR-02 The spine.** The projection takes `Source` and `Types`; an
  ops-sourced pass hands every selected op to the engine's `OpRun` face
  (thing, stream seq, payload) with no fold and no type watcher, and
  refuses an engine without that face. Idempotency on redelivery stays
  (per-thing seq guard).
- **FR-03 Search.** Per-op documents under the same dynamic mapping,
  keyed thing+seq (NUL-separated); ops-mode queries walk every matching
  document, aggregate to things by best op, and report the thing total.
- **FR-04 Semantic.** Per-op chunk sets keyed thing+seq, `fields`
  narrowing op payloads unchanged; queries group documents to things by
  best chunk anywhere. An op with no selected text is no document.
- **FR-05 The gate.** `make check` green, race on, proven against a
  real embedded NATS server — including: effect-none content findable
  through an ops-sourced search and semantic index with thing-level
  hits, one per thing; the live tail indexed with no effects declared;
  the `types` narrowing honored; the config grammar write-side strict;
  graph's refusal; `TestSearchFollowsEffects` (state source) standing
  untouched.
