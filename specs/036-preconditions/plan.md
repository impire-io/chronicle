# Plan 036 — an operation may declare a precondition, and the type's schema is the shape every state keeps

## Layout

```
contract/metakeys.go            OpDef.Precondition
contract/judge.go               Decision PreconditionUnmet, OffSchema; JudgePrecondition; JudgeRecord(rec, state, op); FoldStep judges the merge's result
contract/errors.go              precondition-failed, state-off-schema
contract/sdk.go                 ContractVersion 3.2.0
contract/sdk-contract.json      the artifact at 3.2.0
contract/judge_test.go          the judgement, directly
foldcore/foldcore.go            the two decisions; JudgeRecord takes the state; FoldFingerprint carries the schema and the preconditions
foldcore/pass.go                the two marks, logged
client/ops.go                   ErrPreconditionFailed, ErrStateOffSchema; applyRefusal maps the codes; typeCache.stateOf; preflightAppend judges the state
node/node.go                    DEFINE compiles the precondition
node/apply.go                   the two refusals at the door of a verified write
node/rollup.go                  captureTyped judges as the fold does
node/precondition_test.go       refused pre-flight, refused through the node, marked on a bare publish, bad-schema at DEFINE
cli/types.go                    --precondition on op create, the rebuild narration, op get, the echo, the skeleton
cli/instances.go                the MARK column: marker folds a history from its start
cli/cli_test.go                 TestCLIPreconditions
conformance/fixtures/fold.json  ticket and token; five cases
conformance/scenarios/04-preconditions.json   the live scenario
conformance/scenarios_test.go   precondition-failed, state-off-schema
conformance/README.md           the apply errors
README.md                       the precondition in the five-minute path's explanation
specs/036-preconditions/
```

## Mechanics

- **Order of judgement**, the same in the fold, the SDK's pre-flight and the node's: the operation is defined; the payload fits its schema; the precondition holds against the current state (nil state is the empty object); the effect; for a merge, the result fits the thing schema. The author gate of 0057 precedes all of it in a store that requires verified authors.
- **The state read** in pre-flight is one KV get on the store's state bucket, cached per store as a handle; no entry is no state. It is skipped when the operation has no precondition and either is not a merge or its type has no schema, so a write that needs no judgement costs nothing new.
- **The fingerprint** compacts the thing schema and each effective operation's precondition into the watermark. A store written under 3.1.0 re-folds once on the first boot of this build, because the watermark changes for every typed store; the state is derived and the re-fold is the mechanism the node already runs on a declaration change.
- **The marks in `instance history`** are not read from anywhere: the CLI folds the history it prints, per path, with the type records it reads, and the store's verified-authors setting. A read from a later sequence has no state before it, and the column stays empty.

## Verification

`make check` green — fmt, tidy, build, test (race on), lint. The parity test proves the artifact and the package agree on the decisions and the codes; the fixtures prove the judgement; the node test proves both routes and the marks; the scenario proves the whole over `up`; the CLI test proves the sentences and the column.
