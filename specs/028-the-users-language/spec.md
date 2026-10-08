# Spec 028 — the user's language

**Work ID:** `016-the-users-language` (chronicle-hq research 016, graduated)
**Design:** `chronicle-hq` @ `8ba26fe` (hq PR #58, merged) —
[`00-META/audience.md`](../../../chronicle-hq/00-META/audience.md),
[`00-META/vocabulary.md`](../../../chronicle-hq/00-META/vocabulary.md),
[`03-DECISIONS/0044-chronicle-speaks-the-users-language.md`](../../../chronicle-hq/03-DECISIONS/0044-chronicle-speaks-the-users-language.md),
[`03-DECISIONS/0045-the-cli-is-noun-verb-and-a-list-is-a-list.md`](../../../chronicle-hq/03-DECISIONS/0045-the-cli-is-noun-verb-and-a-list-is-a-list.md),
[`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md),
and the amended [`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md),
[`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md),
[`02-DESIGN/12-the-sdk-contract.md`](../../../chronicle-hq/02-DESIGN/12-the-sdk-contract.md).
**Status:** in progress ([plan.md](plan.md)) — the first of four landings
(`chronicle` → `chronicle-service` → `chronicle-js` → `chronicle-web`).

## What this delivers

The open account plane in the words of its audience. One cutover, pre-v1,
no aliases, no dual-read: the record, the artifact, the META facets, the
client and the CLI all change together.

- **The vocabulary** (0044). A *store* is the container (`log` stays the
  META key prefix and the stream's name, a protocol token); a *thing* is an
  **instance** of a type, named by a **path** `type/id[/child/id…]`
  written with slashes and stored dotted; *aspects* are **children**
  (`children: {name → type}` in the type record); the history policy is
  **`compactable | full`**; an index reads **`state | history`**; the
  compaction act is **taking a snapshot**; a schema-failing operation is
  **invalid**; a principal is a **member** (a person) or a **service
  account** (a machine), a `kind` on the membership record. `effect` stays
  and is always stated. The wire's protocol tokens — subjects, verb names,
  `Op-*` headers, stream and bucket names — do not change.
- **The contract artifact** bumps to `2.0.0`: shape field names, error
  codes and their meanings, the grammars (a path grammar beside the token
  grammar), the interaction names; the Go contract package and the
  artifact test prove the two equal. The conformance suite renames its
  fixtures and scenarios and gains *list the instances of a type* and
  *list an instance's children*.
- **The client** speaks paths and the new nouns: `CreateStore`,
  `ListStores`, `Create` (through the type's `create` operation),
  `CreateFromSnapshot`, `Apply`, `State`, `History`, `Tail`, `Watch`,
  `Snapshot` (the node's compaction), `SaveSnapshot` (the application's),
  `ListInstances` by type, under a parent, and with a field filter —
  bucket scans, never a node verb. Errors name the user's object first.
- **The CLI** (0045, design 07) is one grammar, noun then verb: `store`,
  `type`, `op`, `instance`, `index`, `member`, `service-account`,
  `context`, `account`, plus `login`, `logout`, `up`, `version`, `help`.
  `instance list --type|--in|--where|--sort|--limit` is the sentence that
  was missing. Every list is a table; `--output json|jsonl|yaml`. `--help`
  on every command and `chronicle help [noun]`, exit 0; connection flags
  documented once. Type definitions are YAML or JSON files with a
  commented skeleton from `type init`; `op create` requires `--effect`.
  Errors are the user's words; no retired word appears on any surface,
  and a test proves it.
- **The quick start runs**: `chronicle up` saves and selects a `local`
  context, and a test executes the README's quick-start block, line by
  line, against an in-process `up`.

## Out of scope

- **`store delete`, `type delete`, `member set-role`** — each waits on a
  verb the wire does not have; the CLI says so when asked, and design 07
  is amended to say so. Filed as a deferred follow-up.
- **The managed verbs** (`account create|destroy` as the operator,
  credential issuance) — `chronicle-service`'s landing, in the same
  grammar.
- **The TypeScript SDK and the console** — the next two landings.
- **A fluent SDK surface** (`c.Store("orders").Instance(...)`) — the flat
  client keeps its shape under the new names; the fluent form is the
  SDK contract's call when `chronicle-js` lands.

## Requirements

- **FR-01 Paths.** Every client method that names an instance takes a
  path `type/id[/name/id…]`; `contract.PathTail` and `contract.TailPath`
  convert; a dotted spelling is refused with the path grammar named.
  Hits, visits, edges and listings answer paths.
- **FR-02 The record.** `TypeRecord.Children` (`json:"children"`),
  `HistoryFull = "full"`, `SourceHistory = "history"`,
  `Membership.Kind` (`member` default, `service`); the node validates and
  the fold reads the new names; nothing reads the old.
- **FR-03 The catalog.** `bad-store-name`, `bad-instance`,
  `bad-child-name`, `bad-child-type`, `store-exists`, `no-such-store`,
  `no-such-instance` replace their predecessors; every meaning is in the
  user's words.
- **FR-04 Listing.** `ListInstances(ctx, store, opts...)` yields
  `InstanceInfo{Path, Type, Seq, State}`; `ByType` keeps top-level
  instances of one type, `Under(path)` an instance's direct children,
  `Where(field, value)` filters on a scalar state field; combinable.
- **FR-05 The CLI grammar.** Exactly the sentences of design 07 § the
  grammar; unknown nouns and verbs print the relevant help and exit 1;
  `--help`/`-h` print it and exit 0; `chronicle` alone prints the root
  help, exit 0.
- **FR-06 Tables and output.** Lists print aligned columns; `--output
  json` is one object (a list becomes a JSON array), `jsonl` one object
  per line, `yaml` a document; `instance get` prints the state and its
  sequence.
- **FR-07 Types as files.** `type init <type>` writes a commented YAML
  skeleton; `type create <type> -f FILE` accepts YAML or JSON; `type get`
  prints the readable form; `op create` requires `--effect merge|none`.
- **FR-08 Errors.** No help line, error or result line on the CLI
  contains a retired word (`thing`, `aspect`, `fold`, `rollup`, `birth`,
  `marked`, `tail`, `segment`, `META`, `bucket`, `stream`, `subject`,
  `responder`, a decision number); `--verbose` adds the transport detail.
- **FR-09 The quick start.** `up` saves and selects context `local` when
  none exists; the README's quick-start block runs green in CI.
- **FR-10 The artifact.** Version `2.0.0`; the artifact test and the
  conformance suite pass; the release archive keeps carrying the suite.
