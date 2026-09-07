# Plan 004 — the search index

## Layout

```
contract/metakeys.go          MetaIndex + MetaIndexPrefix; IndexDeclaration;
                              IndexKindSearch; KnownIndexKind
contract/logname.go           ValidateIndexName — the log-name charset rule,
                              no reserved list
client/api.go                 IndexDeclareSubject, IndexDeleteSubject,
                              IndexQuerySubject(log, index); the request/
                              response types; DeclareIndex, DeleteIndex,
                              QueryIndex
internal/registry/registry.go RequireRole — the membership check, extracted
                              from the node so the indexer shares it
internal/foldcore/foldcore.go Judge — the per-op judgment under current
                              declarations, extracted from fold/rollup
internal/node/store.go        requireRole delegates to registry
internal/node/fold.go         applyEffect delegates to foldcore.Judge
internal/node/rollup.go       captureOp delegates to foldcore.Judge
internal/node/node.go         the INDEX.DECLARE / INDEX.DELETE endpoints
internal/index/search/        the chronicle-index-search service: fold to
                              per-thing state, Bleve doc per thing, catch-up
                              gate, query endpoint, type-watcher rebuild
internal/fleet/fleet.go       per-tenant index supervisor: META index.>
                              watcher, one shared indexer conn per tenant
internal/cli/cli.go           chronicle index declare | delete | query
internal/index/search/search_test.go  the indexer end to end
internal/node/index_test.go   the two verbs: roles, kinds, races, delete
internal/fleet/fleet_test.go  declare-through-up starts a serving indexer
internal/cli/cli_test.go      index declare + query round trip
```

## Mechanics

- **`foldcore.Judge(ctx, meta, log, op)`** returns a decision —
  `Merge`, `None`, `UnknownType`, `UnknownEffect`, `BadTypeRecord`,
  `Invalid` — plus detail. It is exactly the shared half of the old
  `applyEffect`/`captureOp`: META type lookup, record decode, schema
  compile + validate, effect normalization. Callers keep their own
  acts and their exact wording — the fold's asserted warn lines
  ("unknown op type ignored", "marked invalid payload", "op before any
  snapshot takes no effect") and rollup's veto reasons ("effect none",
  "unknown type", "marked") are unchanged.
- **The indexer folds in memory**: one ordered consumer over the ops
  family, a per-thing `{state, sawSnapshot}` map, snapshot resets,
  `Merge` applies via `contract.MergePatch`, everything else moves
  nothing — then the thing's state (unmarshaled JSON) is indexed under
  its subject-tail key. Bleve is `NewMemOnly` with the default dynamic
  mapping: every string field analyzed into `_all`, queried with a
  match query (empty query → match-all), limit default 10 cap 100.
- **The catch-up gate is hits' shape**: measure the ops-family backlog
  with a filtered stream-info before consuming; count it down in the
  consume callback; register the micro service only when it reaches
  zero. Until then the query subject has no responder.
- **The rebuild watcher**: the indexer watches META
  `log.<log>.type.>`, remembering each type's normalized effect. The
  initial replay of watcher values seeds the map; after the
  init-done marker, an update whose effect differs tears the fold down
  and re-folds from 1 into a fresh index, atomically swapped under the
  query handler's lock. Schema-only revisions change no effect and
  trigger nothing.
- **The fleet supervisor** mirrors `startNode`: per tenant it opens one
  extra connection (`chronicle-index-<tenant>`), watches META
  `index.>`, and keeps a map of running indexers keyed by META key —
  a put with `kind: search` starts one (idempotent), a delete stops
  it. All of a tenant's indexers share that one connection.
- **The verbs mirror `handleSchemaSet`**: admin role, name validation,
  log-exists check; DECLARE is `meta.Create` (create-if-absent — two
  racing declares settle without a lock, the loser gets
  `index-exists`), kind checked against the vocabulary before the
  write. DELETE reads the key first (`no-such-index` when absent),
  then deletes.
- **Never authority**: the query reply carries thing keys and scores
  only; no state field, by contract.

## Tests

All against a real embedded NATS server, race on, reusing the
node-test harness shape (META provisioned as control would, roles
seeded). The indexer: ops appended before boot are found after Start
returns (boot replay); ops appended after are found shortly (live
tail); a query before catch-up finds no responder; effect-none op
text is not found; a reader may query, a non-member is refused; an
effect change re-folds — text reachable only under the new effect
becomes findable. The verbs: declare answers the query subject;
unknown kind, bad names, missing log, and non-admin are refused;
double declare is `index-exists`; delete then declare succeeds. The
fleet: `Up` + mint + declare through the API starts a serving indexer
without any scheduler. CLI: declare prints the query subject, query
prints hits.
