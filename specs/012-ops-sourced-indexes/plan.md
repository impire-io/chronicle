# Plan 012 — ops-sourced indexes

## Layout

```
contract/source.go         the source vocabulary (SourceState, SourceOps,
                           NormalizeSource), the shared types-narrowing
                           validation, SearchConfig + ParseSearchConfig
contract/graph.go          GraphConfig.Source — "state" admitted, "ops"
                           refused with 0015's rule named
contract/semantic.go       SemanticConfig gains Source + Types
internal/node/index.go     DECLARE validates search config per kind instead
                           of refusing any
internal/index/projection/projection.go
                           Config.Source/.Types; the OpRun face; DocID /
                           DocThing (NUL-keyed — NUL cannot appear in a
                           subject token); ops passes skip the judge, the
                           snapshot gate, and the type watcher
internal/index/search/     searchRun.UpsertOp; queryOps — walk every match,
                           aggregate to things by best op, honest thing
                           total; service.go reads the declaration it never
                           needed before
internal/index/semantic/   semanticRun.UpsertOp (per-op chunk sets, embed
                           once); query groups documents to things via
                           DocThing — one shape for both sources
```

## Mechanics

- **One seam.** The place the state fold discarded effect-none ops is
  where the ops source diverges: before any judging, the pass hands the
  op to the engine and returns. The state path is byte-for-byte what it
  was.
- **No type watcher for ops passes.** Effects don't shape op documents,
  so an effect change cannot make the materialization suspect — the
  0011 § 3 rebuild belongs to the state source alone.
- **Aggregation is at query time, not fold time.** Documents stay
  per-op; things emerge when a query groups by `DocThing`. Search walks
  every matching document so the thing total is honest — in-memory,
  per-log; when that walk is a measured bill, a cheaper answer earns
  its own design. Semantic already walked its whole corpus per query.
- **Search reads its declaration** exactly the way semantic does —
  required, not tolerated-absent: a service materializing an undeclared
  index is a bug, and both supervisor paths (the workload main, the
  in-process backend) only start indexers for declared indexes.

## Tests

All against a real embedded NATS server, race on. Search:
`TestSearchOpsSource` (history-only text findable, thing-level hits with
an honest total, live tail, `types` narrowing blind to other types) and
`TestParseSearchConfigIsWriteSideStrict`; the two pre-existing search
tests gain the declaration they now need; `TestSearchFollowsEffects`
otherwise untouched — the state source still cannot see effect-none
content. Semantic: `TestSemanticOpsSource` (two matching ops, one hit;
best-op ranking; the live tail embeds with no effects). Graph:
`ops source` joins the refusal matrix, explicit `state` accepted. Node:
the DECLARE matrix — ops-sourced search accepted, graph-from-ops
refused, garbage still refused.
