# Spec 013 — type records

**Work ID:** `007-fewer-concepts` (chronicle-hq research 007)
**Design:** `chronicle-hq` @ `096d36b` —
[`03-DECISIONS/0021-the-type-is-the-unit-of-definition.md`](../../../chronicle-hq/03-DECISIONS/0021-the-type-is-the-unit-of-definition.md)
and [`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md)
§ type records, § the state buckets (fold rules).
**Status:** specified.

## What this delivers

The type becomes the one thing a user defines. Today vocabulary is
per-op records (`log.<log>.type.<op.type>` = `{revision, schema,
effect}`, set by `SCHEMA.SET`), a thing is an opaque subject tail with
untyped state, and the CLI's noun is `schema` — the abstraction, not
the domain.

- **One META record per type**: `log.<log>.type.<type>` =
  `{revision, schema, history, aspects, operations}`. `<type>` follows
  the log-name grammar (`[a-z0-9-]+`) — it can never collide with the
  dotted op-type keys it replaces. Defining a type is one act that sets
  all facets; re-defining bumps `revision` under KV-revision CAS.
- **`operations` subsumes the op-type records** — each entry is
  `{schema, effect}` keyed by op-type string (natural dots kept). An
  operation is defined inside exactly one type; a definition spanning
  types is unrepresentable. Effect semantics are 0011's, unchanged.
- **`schema` is the thing's shape** — new capability: pre-flight on
  snapshot payloads, projections marking state that fails it. Read-side
  only, no admission gate.
- **A typed thing is addressed by its tail**: the first `<type>.<id>`
  pair names its type. A tail that resolves to no declared type is
  **untyped** — the vocabulary-less floor: no thing schema, every op
  judged unknown (effect none), the 0011 § 4 compaction veto in force.
  Latest declaration wins: declaring the type later types existing
  things retroactively on re-fold.
- **`CHRON.API.TYPE.DEFINE` replaces `CHRON.API.SCHEMA.SET`** — admin
  role, write-side strict, same CAS discipline; the rebuild trigger
  widens from effect changes to any change of effects, aspects, or
  history. The CLI reworks around the domain nouns: `type define`,
  `type inspect`, `type list`, operations defined within the type;
  `schema set` is removed.
- **The cutover is one-off** (Daan's call, 2026-09-17): the node reads
  only the new record shape; dev installs re-declare. No dual-read.

## Out of scope

- **Aspect resolution, marking, and the roll-up gate change** —
  spec 014 ([0022](../../../chronicle-hq/03-DECISIONS/0022-an-aspect-is-a-typed-thing-under-its-parents-prefix.md)).
  This spec carries the `aspects` facet in the record and validates it
  write-side; nothing consumes it yet.
- **The state-index declaration** — spec 015
  ([0023](../../../chronicle-hq/03-DECISIONS/0023-state-is-a-declared-index.md)).
- **Account-wide types, schema evolution, config-edit verbs** — parked
  by the records.

## Requirements

- **FR-01 The record.** `contract` gains `TypeRecord{Revision, Schema,
  History, Aspects, Operations}` with `OpDef{Schema, Effect}`;
  `MetaLogType(log, type)` keeps the key prefix; type-name and aspect-
  segment grammar is the log-name grammar; `history` vocabulary is
  0019's (`compactable` default); the reserved `snapshot` op key is
  refused inside `operations`. The old `TypeSchema` shape and its
  writers/readers are removed.
- **FR-02 The verb.** `CHRON.API.TYPE.DEFINE` (admin): write-side
  strict on history, effects, aspect segments, and type-name grammar;
  compiles the thing schema and every operation schema before
  recording; read-current + revision+1 under KV-revision CAS; returns
  the recorded revision. `SCHEMA.SET` and its handler are removed. A
  change to effects, aspects, or history reports "changed" and
  triggers the log rebuild exactly as an effect change does today.
- **FR-03 Resolution.** One shared resolver: tail → `(type, ok)` for
  the first `<type>.<id>` pair against the log's type records (full
  pair-chain resolution arrives with spec 014; here a two-token tail
  whose first token is a declared type is typed, anything else
  untyped). The node fold and the projection spine judge ops through
  the resolved type's `operations`; an untyped thing's ops judge
  unknown. The projection's type watcher reads the new record shape.
- **FR-04 Pre-flight.** SDK `CreateThing` validates the snapshot's
  `state` against the resolved type's thing schema; `Append` on a
  typed thing refuses locally an op not in the type's `operations` and
  validates the payload against the op's schema. Untyped things keep
  today's no-schema behavior: publish, readers judge.
- **FR-05 The CLI.** `type define <log> <type> --file <json>` (the
  whole record in one act), `type inspect <log> <type>`,
  `type list <log>`; `schema set` removed; `thing create` and `append`
  keep their shape and gain the pre-flight of FR-04.
- **FR-06 The gate.** `make check` green, race on, against a real
  embedded NATS server — including: a defined type's merge op folds
  state for `<type>.<id>` things; an untyped tail stays on the
  snapshot floor; declaring the type later re-folds it typed
  (retroactivity); TYPE.DEFINE's write-side strictness matrix
  (bad history, bad effect, bad segment grammar, reserved snapshot);
  revision CAS under concurrent defines; pre-flight refusals; the
  effect-change rebuild still proven.
