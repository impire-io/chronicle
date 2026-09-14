# Spec 008 — the graph index kind

**Work ID:** `chronicle-5` (tracker item chronicle-5)
**Design:** `chronicle-hq` @ `7f5f9c0` —
[`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md)
(§ the declaration: `{kind, config?}`; § the graph kind; § the query
surface) and
[`03-DECISIONS/0015-the-graph-kind.md`](../../../chronicle-hq/03-DECISIONS/0015-the-graph-kind.md).
**Status:** in progress on this branch ([plan.md](plan.md)).

## What this delivers

The second index kind, end to end — and the projection spine extracted,
because graph would have been its second copy.

- **`internal/index/projection`**: the state-materialization spine
  pulled out of the search service — replay-from-1 with the backlog
  countdown, the live tail, the effect-change watcher, the
  rebuild-and-swap manager, registered-after-caught-up readiness — with
  the engine behind a two-method seam (`NewRun`/`Upsert`). Search
  becomes its first consumer unchanged in behavior; graph its second.
  A third copy was the drift hazard `foldcore`'s extraction named; this
  is the same move one layer up.
- **The declaration grows config**: `IndexDeclaration{kind, config?}`,
  `INDEX.DECLARE` accepting and validating it write-side strict per
  kind — graph requires well-formed edge rules, search still refuses
  any config (no-knobs stands). The graph workload reads its own
  declaration from META at boot; delete + declare is a workload
  lifecycle, so config never hot-reloads.
- **The graph kind** (`internal/index/graph`): edge rules
  `{field, label?}` with dotted paths evaluated into thing state —
  arrays traversed element-wise, strings at the leaf are edges, anything
  else counts as skipped and never errors. Out-edges recomputed
  wholesale per fold step; forward and reverse adjacency maps under one
  lock; targets opaque. Queries on the standard subject with the graph
  payload: `op: neighbors` (direction, label filter, stable order,
  limit/offset) and `op: walk` (breadth-first, cycle-safe, depth capped
  at 6 and the cap stated in the reply).
- **Fleet plumbing**: workload kind `index-graph` through the bridge
  (report kind `graph`), the executor's vocabulary, backend zero, and
  the workload binary — each a case added where 0014 left the seam.
- **SDK and CLI**: `DeclareIndex` carries config
  (`chronicle index declare <log> <index> --kind graph --config JSON`);
  `GraphNeighbors`/`GraphWalk` on the client;
  `chronicle graph neighbors|walk`.

## Out of scope

Per 0015: inferred edges, path/pattern queries, cross-log in-edge
fan-out, any graph store. Per-field search config (search stays
no-knobs). The semantic kind — its own item, behind [0016](../../../chronicle-hq/03-DECISIONS/0016-the-semantic-kind.md).
Field-existence validation at DECLARE (soft, like the fold's own
tolerance).
