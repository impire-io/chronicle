# The conformance suite

What "supports chronicle" means for an SDK ([design 12](../../chronicle-hq/02-DESIGN/12-the-sdk-contract.md)): an SDK declares the contract version it implements (`contract/sdk-contract.json`, `version`) and passes this suite of that version. The suite ships with every release archive; an SDK's CI fetches the archive and runs both halves against it. The words are the [vocabulary](../../chronicle-hq/00-META/vocabulary.md)'s: a store, its types, their instances named by paths (`type/id`, with `/name/id` for each child), children, snapshots.

## Golden fixtures — `fixtures/`

JSON in, JSON out, for every pure rule an SDK re-implements. A runner walks the directory and asserts each case against the SDK's own functions:

| File | Rule | The SDK function under test |
|---|---|---|
| `names.json` | name validation: store, index, type, principal, the stored instance tail, the path | validate a name of that kind; `valid` says whether it passes |
| `derivations.json` | the path ↔ tail conversion; stream, bucket, subject and META key derivations | derive each field from `store` and `path` |
| `headers.json` | the operation record ↔ its headers | encode the op to headers; parse the headers back to the op |
| `resolve.json` | path resolution through the children maps | resolve `tail` under `types` to `kind` and `type` |
| `merge.json` | RFC 7386 merge patch, the effect `merge` | apply `patch` to `target`, expect `result` |
| `fold.json` | the fold step | under `types`, apply `ops` to an absent state, expect `decisions` and `state` |
| `guard-retry.json` | the retry judgement after an expected-sequence refusal | compare the subject's last op ID with the retried op's; `landed` says which way |

The Go runner is [`fixtures_test.go`](fixtures_test.go).

## Live scenarios — `scenarios/`

A sequence of sentences against `chronicle up` in the open form, with the observations expected. Each step is `{"do": <sentence>, ...arguments, "expect": {...}}`; derived observations (`get`, `query`, `list.instances`, `history`) are polled until they hold or the timeout passes, because state and indexes are projections that trail the history. The subscribe steps (`watch`, `tail`, `watch.declarations`) open the iterator, perform the write named in `then`, and expect the item to arrive.

| Sentence | Interaction | Expectations |
|---|---|---|
| `store.create`, `type.define`, `index.declare`, `index.delete`, `snapshot` | the control verbs | `error` (a catalogued code or a typed client error), `taken` |
| `instance.create.snapshot`, `instance.create`, `apply`, `apply.guarded` | a create from a snapshot, a create through the type's `create`, an operation, an operation with the expected sequence | `error`: `instance-exists`, `instance-moved`, `preflight`, `undefined-operation` |
| `get` | the state read | `state`: every named key equals |
| `history`, `tail` | history and the live tail | `types` in order; `type` arrives |
| `query` | the streamed reply | `instances` (a set), `total`, `count`, `error: no-responder` |
| `list.stores`, `list.types`, `list.indexes`, `list.members`, `list.instances` | the bucket scans | `items` (a set); `list.instances` narrows by `type`, `in` (a parent's direct children) and `where` (a field filter) |
| `watch`, `watch.declarations` | the KV watches | `first` (what is at rest), `contains` / `arrives` (what lands) |

The Go runner is [`scenarios_test.go`](scenarios_test.go): boot `up`, connect the local user, run each step.

## Running it in Go

```
go test ./conformance/
```
