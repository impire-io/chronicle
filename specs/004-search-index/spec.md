# Spec 004 — the search index

**Work ID:** `29` (tracker item 29)
**Design:** `chronicle-hq` @ `0536b42` —
[`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md)
(the whole doc: declarations, the search kind, the query surface,
scheduler-less supervision) and
[`03-DECISIONS/0012-search-is-the-first-index-kind.md`](../../../chronicle-hq/03-DECISIONS/0012-search-is-the-first-index-kind.md).
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

The first index kind, end to end. The `index.` META prefix was reserved
and unwritten; nothing declared, materialized, or served an index.

- **Declaration verbs on the node**: `CHRON.API.INDEX.DECLARE` writes
  `index.<log>.<index>` create-if-absent with value `{kind}`, admin
  role, refusing unknown kinds (write-side strict, the 0011 split),
  missing logs, and names outside the log-name grammar. The response
  carries the query subject the index will serve.
  `CHRON.API.INDEX.DELETE` removes the key; the supervisor retires the
  workload. Changing a declaration is delete + declare.
- **The indexer**: `chronicle-index-search`, one micro service per
  declared index, holding the tenant's service user only. It folds the
  log under the current declarations — the same 0011 rules as the
  node's fold, via the shared judge — into per-thing state, and indexes
  each thing's state as one document (embedded Bleve, in-memory, every
  string field under default analysis). It replays from sequence 1 at
  boot and registers its query endpoint **only after catching up** with
  the stream head observed at start: a responder implies a current
  index.
- **The query surface**: `CHRON.API.INDEX.QUERY.<log>.<index>` —
  request `{principal, query, limit, offset}`, any registry role;
  reply `{hits: [{thing, score}], total}`. The index is never
  authority: a hit names a thing; state is the state bucket's.
- **Suspect-state rebuild**: a changed effect makes the indexer's
  derivation suspect exactly as it does the state bucket's (0011 § 3).
  The indexer watches the log's `type.` keys and re-folds in place —
  fresh index, replay from 1 — when a type's declared effect changes.
- **Scheduler-less supervision**: `chronicle up`'s fleet watches each
  tenant's META `index.>` keys and starts/stops in-process indexers to
  match the declarations — 0012's stand-in for the 0004 scheduler, on
  one shared indexer connection per tenant (the hits 0006
  connection-count lesson).
- **SDK and CLI plumbing**: `DeclareIndex`, `DeleteIndex`,
  `QueryIndex` on the client; `chronicle index declare|delete|query`.

## Shared judgment, extracted

The per-op judgment under the current declarations — type lookup,
schema validation, effect decision — existed twice (the fold's
`applyEffect`, rollup's `captureOp`) and the indexer would have been a
third copy. It is extracted once (`internal/foldcore.Judge`) and all
three callers map its decisions to their own acts: the fold warns and
writes, rollup vetoes, the indexer warns and indexes. The registry
role check moves the same way (`internal/registry.RequireRole`) so the
indexer's query endpoint checks roles without reaching into the node.
Behavior is unchanged: the asserted log lines and veto reasons stay.

## Out of scope

Per the design's "what this does not do": per-field mappings, filters,
or analyzers; pagination cursors beyond limit/offset; persisted index
files; graph and semantic kinds; op-history search; query fan-out or
cross-log search; the 0004 scheduler and its backends. Editing a
declaration in place (it is delete + declare by contract).
