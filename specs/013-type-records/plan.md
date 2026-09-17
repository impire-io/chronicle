# Plan 013 — type records

Built together with spec 014: the resolver is one mechanism, so the
pair walk landed complete here rather than the spec's two-token interim,
and 014's marking/gates ride the same commit series.

## Layout

```
contract/metakeys.go     TypeRecord{revision, schema, history, aspects,
                         operations} + OpDef; MetaLogType takes the type
                         name (log-name grammar — never collides with the
                         retired dotted op-type keys)
contract/resolve.go      Resolution + ResolveTail: the pure pair walk over
                         a TypeLookup closure — shared by fold, rollup,
                         projection, and SDK pre-flight
contract/logname.go      ValidateTypeName (log-name grammar, no reserved
                         list; aspect segments use it too)
contract/fleet.go        FleetTypeRecords: one `workload` type carrying
                         the three custody ops; FleetWorkloadType
client/api.go            TYPE.DEFINE subject + request/response;
                         DefineType(TypeDefinition); SCHEMA.SET removed
client/ops.go            typeCache (fresh record reads, compiled-schema
                         cache per revision); preflightAppend refuses
                         undefined operations and undeclared aspects,
                         validates payloads; preflightSnapshot validates
                         birth/save state against the thing schema;
                         ErrUndeclaredAspect, ErrUndefinedOperation
client/read.go           GetType, ListTypes — KV reads: definitions are
                         discoverable data, never served through a verb
internal/foldcore/       the two halves: ResolveThing/JudgeThing (tenant),
                         JudgeAs (by name — the fleet), JudgeRecord,
                         JudgeSnapshot, FoldFingerprint
internal/node/store.go   recordType: read-current, revision+1 under KV
                         CAS; "changed" = fold fingerprint moved
internal/node/node.go    handleTypeDefine: write-side strict on every
                         facet's vocabulary; rebuild on fingerprint change
internal/node/fold.go    resolve-first apply: undeclared aspects marked
                         whole, typed snapshots validated, untyped things
                         on the vocabulary-less floor
internal/node/rollup.go  the three-stage gate (spec 014) — log, type,
                         per-op veto for untyped only
internal/index/projection/  resolve-first state pass; the type watcher
                         compares fold fingerprints, so schema-only
                         revisions trigger nothing
internal/workloads/      seeds FleetTypeRecords; the fleet fold resolves
                         by thing family (JudgeAs), not pair addressing
internal/cli/cli.go      type define/inspect/list; schema set removed
```

## Mechanics

- **One resolver, two judges.** `ResolveTail` is pure over a lookup
  closure, so the SDK (client package, cannot import internal) and every
  projection share the identical walk. Judgment splits into
  resolve-then-judge (tenant) and judge-by-name (fleet): the fleet's
  custody tails (`workload.<tenant>.<workload>`) predate the pair grammar
  and carry two id tokens, and its vocabulary is chronicle's own code —
  resolving by family keeps the fleet on the shared declarations without
  bending the tenant grammar around it.
- **The fingerprint is the rebuild trigger.** History, aspects, and each
  effective operation's effect — normalized, none-effects dropped. A
  schema-only revision changes no derivation and triggers nothing; a
  first definition carrying anything effective reads as changed against
  the nil baseline.
- **Snapshot ops bypass the operation lookup** in pre-flight: the
  snapshot type is the pattern's own vocabulary, any member may publish
  one; its state validates like a birth's where a type resolves.
- **The cutover is total.** `TypeSchema`, `SCHEMA.SET`, and the per-op
  key shape are gone; nothing reads the old value shape. Existing
  installs re-declare (0021 § 6 — nothing hosted, nothing to migrate).

## Tests

Migrated to the type model across node, search, semantic, and fleet
suites: typed things address as `<type>.<id>`, vocabulary lands through
`DefineType`, revision and write-side-strictness assertions moved onto
the type record (including uncompilable thing and operation schemas, and
read-back via GetType/ListTypes). Pre-flight coverage: undefined
operation refused on a typed thing, untyped things publish freely,
schema violations refused as before. `TestEffectChangeRebuildsState`
re-defines the whole type; unknown-type tolerance now enters raw (the
SDK refuses what the vocabulary excludes).
