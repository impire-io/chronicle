# Spec 014 — aspects

**Work ID:** `007-fewer-concepts` (chronicle-hq research 007)
**Design:** `chronicle-hq` @ `096d36b` —
[`03-DECISIONS/0022-an-aspect-is-a-typed-thing-under-its-parents-prefix.md`](../../../chronicle-hq/03-DECISIONS/0022-an-aspect-is-a-typed-thing-under-its-parents-prefix.md)
and [`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md)
§ the state buckets, [`02-DESIGN/04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md)
§ the node's duties (rollups), [`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md)
§ what an index materializes.
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge. Builds on spec 013.

## What this delivers

Protection-by-address replaces protection-by-veto. Today a thing that
accumulates history-bearing ops (`effect: none`) is permanently
uncompactable — 0011 § 4 vetoes the roll-up on every such tail. Now the
comment is its own thing under the invoice's prefix, folding by its own
type's declaration, and the invoice compacts.

- **Full pair-chain resolution**: `<type>.<id>(.<segment>.<id>)*`,
  resolved left-to-right — the first pair against the log's types, each
  further pair through the current type's `aspects` map
  (`{segment → type}`). Odd-length tails are subscribe prefixes, not
  things: untyped. One type may attach under several segments.
- **Enforcement is pre-flight plus fold-side marking** — the
  schema-validation pattern reused. The SDK refuses a birth whose
  created type is not in the parent type's aspects map; the fold
  **marks** such a subject, and a marked subject takes no effect in the
  state bucket or any state-sourced index. Ops-sourced indexes read its
  history as it is: declarations shape state, never history's
  visibility. Latest declaration wins: adding the segment redeems
  marked subjects retroactively on re-fold; removing it re-marks them.
- **The roll-up gate becomes three-stage**: the log's `history`
  declaration first (0019, unchanged — server-hard); the thing's
  resolved type second — `preserved` skips the sweep and declines the
  verb with the reason named, `compactable` compacts **regardless of
  effect coverage** (the per-op veto retires on typed things: the type
  author declared this history absorbable); the per-op effect-coverage
  veto stands only where no type resolves.

## Out of scope

- **A hard server-side attach refusal** — priced and declined
  (research 007 doc 03); a birth-through-a-verb stays additive later.
- **Discovery verbs** — a reader walks the aspects map itself; declared
  means possible, not present.
- **The state-index declaration** — spec 015.

## Requirements

- **FR-01 Resolution.** The spec-013 resolver grows the full chain:
  even-length tails resolve pair by pair; the outcome is one of
  *typed* (with the resolved type), *untyped* (no declared first type,
  or an odd length), or *undeclared-aspect* (a pair whose segment the
  parent type's aspects map lacks — the mark). Multi-level chains
  resolve recursively; resolution is pure read-side vocabulary.
- **FR-02 Marking.** The node fold and the state-sourced projection
  pass drop marked subjects from materialization (state bucket entry
  absent, no document), symmetrically with schema-invalid payloads:
  the log keeps everything. An aspects-map change triggers the same
  rebuild as an effect change (wired in spec 013), redeeming or
  re-marking on re-fold. Ops-sourced passes are untouched.
- **FR-03 Pre-flight.** SDK `CreateThing` under a prefix resolves the
  parent chain from META and refuses locally when the created pair's
  segment is not declared by the parent's type; a typed create also
  keeps spec 013's thing-schema validation. Raw publishes still land —
  the fold marks them.
- **FR-04 The gate order.** `rollupThing` and the sweep: 0019's log
  gate unchanged; then the resolved type's `history` — `preserved`
  declines with the reason named, `compactable` compacts without the
  per-op veto; the existing effect-coverage veto applies only to
  untyped subjects. Aspect subjects are swept independently of their
  parents — per-subject roll-up is already the unit.
- **FR-05 The gate.** `make check` green, race on, real embedded NATS —
  including: an invoice-with-comments scenario where the parent
  compacts while a `preserved` comment aspect keeps full history; an
  undeclared aspect marked out of state and state-sourced search but
  findable ops-sourced; redemption after the parent type gains the
  segment; the SDK attach refusal; a `compactable` typed thing with
  stray `none` ops compacting by declaration; an untyped subject still
  vetoed; multi-level chains folding independently.
