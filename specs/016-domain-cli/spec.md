# Spec 016 — the CLI speaks the domain

**Work ID:** `chronicle-17` (tracker item; chronicle-hq research 008)
**Design:** `chronicle-hq` @ `3f461bb` (hq PR #11 — re-cite on merge) —
[`03-DECISIONS/0025-the-cli-speaks-the-domain.md`](../../../chronicle-hq/03-DECISIONS/0025-the-cli-speaks-the-domain.md)
and [`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md).
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

The fewer-concepts model (0021–0023) spoken by the CLI instead of
translated back into wire vocabulary. Nothing here touches the wire
contract: no new API verbs, no header or subject changes; discovery is
reads of definitions and state at rest.

- **Contexts and the selection.** A context is chronicle's own record
  — `{url, creds path, selected log}` — under the user config dir;
  `context save|select|list|show|rm`, `log select`. Every tenant-plane
  sentence drops both the `--creds` flag and the `<log>` positional:
  resolution is flags over env (`CHRONICLE_CONTEXT`, `CHRONICLE_LOG`)
  over the selected context, and no answer anywhere is a teaching
  error naming the fixes. `tenant create` and `member add`
  save-and-select a context for the creds they mint; `log create`
  selects the log it made; `context show` names tenant, url, and log.
- **The sentences.** `create`, `do`, `get`, `history`, `rollup` — bare
  verbs on things; `append`, `state`, `replay`, and the `thing` noun
  are retired, not aliased. `create` is `do` with the birth guard: it
  invokes the type's `create` operation by convention (`--op` names
  another), payload judged by that operation's schema, published with
  expected sequence 0. No constructor marker — birth is an invocation
  mode; a typed thing whose type declares no `create` gets a teaching
  error; an untyped tail keeps the raw snapshot birth with `--payload`
  as its birth state.
- **Vocabulary managed where it lives.** `operation` (alias `op`, one
  verb) `define|list|inspect|rm` compose 0021's one-act `TYPE.DEFINE`
  by read-modify-write under the record's revision CAS. The CLI
  narrates consequences, then acts: an effect change names the rebuild
  it made suspect; `rm` states the none-with-warning re-fold. `type
  init` scaffolds a definition file (schema stub, history default,
  empty aspects, a `create` operation); `type define` echoes back what
  it defined; `type inspect` is readable by default, `--json` for the
  record.
- **Discovery.** `log list`, `index list` (name, kind — the state
  index included, its undeletability said), `things [prefix]` (state
  bucket keys, the fold watermark excluded). All data-at-rest reads;
  the client gains them.
- **One query.** `query <index> [args]`: the declared kind, read from
  META, shapes the arguments — text for search/semantic, `--from` for
  graph where the depth argument decides neighbors (absent) from walk
  (present). Bare, it says what the index accepts. `index query`,
  `semantic query`, `graph neighbors|walk` retire. The state index
  stays outside `query` — `get` is its read.
- **Two planes, visible.** Sectioned help: run a fleet · own its
  tenants (fleet dir) · define vocabulary · work with things · find
  things (your context).

## Out of scope

- **Wire or SDK-surface renames** — `Append`, `CreateThing`,
  `SaveVersion` stand; the CLI renames, the wire and the Go API do not.
- **Shell completion, interactive prompts** — the grammar first.
- **The web panel and non-Go SDKs** (0005 arcs).

## Requirements

- **FR-01 The context store.** Chronicle's own records under the user
  config dir (`CHRONICLE_CONFIG_HOME` overrides the root — tests and
  scripts isolate); `context save <name> --creds F [--url U]`,
  `select`, `list`, `show`, `rm`; `log select <log>` writes the
  selection on the selected context. Resolution, field-wise: `--url` /
  `--creds` / `--log` beat `CHRONICLE_CONTEXT`-named and
  `CHRONICLE_LOG` values beat the selected context; a missing url
  falls back to the `--dir` fleet's recorded url as today. A sentence
  needing a log or creds and finding none is refused with both fixes
  named (`chronicle log select <log>` / `--log`; `chronicle context
  save` / `--creds`). `tenant create` and `member add` write and
  select a context named `<tenant>-<principal>` pointing at the creds
  they just minted; `log create` records its log as the selection.
- **FR-02 The sentences.** `create <thing> [--payload JSON] [--op
  <operation>]`, `do <thing> <operation> [--payload JSON] [--parents]
  [--expect-seq]`, `get <thing>`, `history <thing>`, `rollup <thing>`
  — no `<log>` positional anywhere; `append`, `state`, `replay`,
  `thing create`, `thing rollup` are gone from dispatch and usage.
  `create` on a typed tail resolves the operation (`create` unless
  `--op`), pre-flights the payload against its schema, and publishes
  it birth-guarded (expected sequence 0) — refused as "already exists"
  when the guard trips on someone else's birth, idempotent on a retry
  of its own; on an untyped tail it publishes the raw snapshot birth
  with `--payload` as the state. A typed tail whose type declares no
  `create` operation is refused with the type's operations listed.
- **FR-03 Vocabulary.** `type init [<type>]` writes the skeleton to
  stdout; `type define <type> --file|--def` echoes operations (with
  effects), aspects, and history on success; schema/effect/facet
  errors name the field and the vocabulary. `operation`/`op`
  `define <type> <operation> --schema F|JSON [--effect E]`,
  `list <type>`, `inspect <type> <operation>`, `rm <type>
  <operation>`: read the type, change the operations facet, redefine
  whole; the effect-change and rm consequences are printed before the
  write. `type inspect` prints the readable summary (shape fields,
  operations table, aspects map, history) by default and the raw
  record under `--json`.
- **FR-04 Discovery.** Client: `ListLogs` (META `log.<log>.config`
  keys), `ListIndexes` (META `index.<log>.*` declarations with kind),
  `ListThings` (state bucket keys, `=fold` excluded, optional prefix),
  `GetIndexDeclaration`. CLI: `log list`, `index list`, `things
  [prefix]` on top of them — readable default, `--json` everywhere a
  machine might read.
- **FR-05 Query.** `query <index> [text...] [--from T] [--depth N]
  [--limit N] [--offset N]`: kind `search`/`semantic` route to the
  existing query calls with the joined text; kind `graph` requires
  `--from` and routes to neighbors (no `--depth`) or walk (`--depth`
  given); kind `state` and unknown kinds are declined with the reason;
  `query <index>` with no arguments prints what the index accepts.
- **FR-06 The gate.** `make check` green, race on, real embedded NATS.
  The design's day-in-the-life runs verbatim as the spine wire test:
  tenant create (context saved and selected) → log create (selected)
  → type define → create through the constructor → do → get →
  history → rollup → index declare → query — plus the refusals:
  no-selection teaching error, typed-create-without-constructor, stale
  `--expect-seq` on `do`, untyped rollup veto. The README quick start
  is rewritten to the new sentences.
