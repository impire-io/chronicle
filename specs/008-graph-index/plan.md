# Plan 008 — the graph index kind

1. **Contract**: `IndexDeclaration.Config`, `IndexKindGraph`,
   `GraphConfig`/`GraphEdgeRule` with `ParseGraphConfig` validation,
   the graph query payloads (`op: neighbors|walk`, replies), workload
   kind `index-graph`. Client: `IndexDeclareRequest.Config`, graph
   request/response types, `DeclareIndex` signature grows config,
   `GraphNeighbors`/`GraphWalk`.
2. **`internal/index/projection`**: extract the search service's spine
   behind `NewRun() (Run, error)` / `Run.Upsert(thing, state)`; search
   refactored onto it, behavior and log lines preserved, its tests
   untouched and green.
3. **`internal/index/graph`**: the edge evaluator (dotted paths,
   array traversal, unit-tested), the adjacency run, the two query
   verbs, the service reading its declaration from META at boot.
4. **Node**: DECLARE accepts config, validates per kind (graph rules
   parse; search refuses config), stores it; the bridge report is
   unchanged (kind rides it already).
5. **Fleet plumbing**: bridge handler maps kind graph → `index-graph`;
   executor `supports`; backend zero and `chronicle-workload` cases.
6. **CLI**: `index declare --config`, `graph neighbors`, `graph walk`.
7. **Wire tests**: graph service test mirroring the search one
   (references in state, neighbors both directions, walk depth,
   edge replacement on state change); fleet-level declared-graph-serves
   test; the full gate; live read through `chronicle up`.
