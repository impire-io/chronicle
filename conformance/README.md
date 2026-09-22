# The conformance suite

What "supports chronicle" means for an SDK ([design 12](../../chronicle-hq/02-DESIGN/12-the-sdk-contract.md)): an SDK declares the contract version it implements (`contract/sdk-contract.json`, `version`) and passes this suite of that version. The suite ships with every release archive; an SDK's CI fetches the archive and runs both halves against it.

## Golden fixtures — `fixtures/`

JSON in, JSON out, for every pure rule an SDK re-implements. A runner walks the directory and asserts each case against the SDK's own functions:

| File | Rule | The SDK function under test |
|---|---|---|
| `names.json` | name validation: log, index, type, principal, thing | validate a name of that kind; `valid` says whether it passes |
| `derivations.json` | stream, bucket, subject and META key derivations | derive each field from `log` and `thing` |
| `headers.json` | the operation record ↔ its headers | encode the op to headers; parse the headers back to the op |
| `resolve.json` | tail resolution through the aspects maps | resolve `thing` under `types` to `kind` and `type` |
| `merge.json` | RFC 7386 merge patch, the effect `merge` | apply `patch` to `target`, expect `result` |
| `fold.json` | the fold step | under `types`, apply `ops` to an absent state, expect `decisions` and `state` |
| `guard-retry.json` | the retry judgement after a guard refusal | compare the subject's last op ID with the retried op's; `landed` says which way |

The Go runner is [`fixtures_test.go`](fixtures_test.go).

## Live scenarios — `scenarios/`

A sequence of sentences against `chronicle up` in the open form, with the observations expected. Each step is `{"do": <sentence>, ...arguments, "expect": {...}}`; derived observations (`get`, `query`, `list.things`, `history`) are polled until they hold or the timeout passes, because state and indexes are projections that trail the log. The subscribe steps (`watch`, `tail`, `watch.declarations`) open the iterator, perform the write named in `then`, and expect the item to arrive.

| Sentence | Interaction | Expectations |
|---|---|---|
| `log.create`, `type.define`, `index.declare`, `index.delete`, `rollup` | the control verbs | `error` (a catalogued code or a typed client error), `rolled` |
| `create`, `create.op`, `do`, `do.guarded` | append: a snapshot birth, a birth through the type's `create`, an operation, a guarded operation | `error`: `thing-exists`, `thing-moved`, `preflight`, `undefined-operation` |
| `get` | the state read | `state`: every named key equals |
| `history`, `tail` | history and the live tail | `types` in order; `type` arrives |
| `query` | the streamed reply | `things` (a set), `total`, `count`, `error: no-responder` |
| `list.logs`, `list.types`, `list.indexes`, `list.members`, `list.things` | the bucket scans | `items` (a set) |
| `watch`, `watch.declarations` | the KV watches | `first` (what is at rest), `contains` / `arrives` (what lands) |

The Go runner is [`scenarios_test.go`](scenarios_test.go): boot `up`, connect the local user, run each step.

## Running it in Go

```
go test ./conformance/
```
