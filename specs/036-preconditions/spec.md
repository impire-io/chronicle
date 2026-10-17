# Spec 036 — an operation may declare a precondition, and the type's schema is the shape every state keeps

**Work ID:** `chronicle-75` (chronicle-hq tracker item chronicle-75; the build of chronicle-hq decision 0059, first repo in the landing order)
**Design:** `chronicle-hq` @ `23d173a` (hq PR #94) —
[`03-DECISIONS/0059-an-operation-may-declare-a-precondition-and-the-types-schema-is-the-shape-every-state-keeps.md`](../../../chronicle-hq/03-DECISIONS/0059-an-operation-may-declare-a-precondition-and-the-types-schema-is-the-shape-every-state-keeps.md),
filed by hits-hq decision 0018 out of its research 005, and the amended designs
[`03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md) § type records, § the state buckets, § what this does not do;
[`07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md) § the grammar, § types and operations, § instances;
[`12-the-sdk-contract.md`](../../../chronicle-hq/02-DESIGN/12-the-sdk-contract.md) § the caller and the verified write, § the conformance suite;
the [vocabulary](../../../chronicle-hq/00-META/vocabulary.md)'s *operation* row.
**Status:** built on branch `chronicle-75` ([plan.md](plan.md)).

## What this delivers

A type can say when an operation is legal, and the shape its state keeps is kept. Contract **3.2.0** — additive, minor.

1. **The precondition facet.** An operation's entry in the type record gains `precondition`: a JSON Schema the instance's **current state** must satisfy for the operation to take effect (`contract.OpDef.Precondition`). Absent, the operation applies in any state, as before. `TYPE.DEFINE` refuses a precondition that does not compile with `bad-schema`, as it refuses an operation schema that does not.
2. **The fold judges it.** `contract.JudgeRecord` takes the current state and judges, after the payload's schema and before the effect, the precondition against it — the empty object when the thing has none yet, so a precondition on `create` sees what a create sees. The decision **`precondition-unmet`** marks the operation: kept in the history, moves nothing. An operation with effect `none` is judged too; its mark is what a history read shows.
3. **The schema is the shape every derived state keeps.** `contract.FoldStep` judges the state a `merge` produces against the thing schema as it already judged a snapshot's; a merge whose result fails it is **`off-schema`**: marked, the state standing where it was. The rollup gate (`node/rollup.go`, `captureTyped`) judges alike, so a snapshot keeps exactly what folded state keeps. `foldcore.FoldFingerprint` now carries the thing schema and each effective operation's precondition, so a revision of either makes derived state suspect and the node re-folds — one re-fold of every store on the first boot of this build.
4. **Pre-flight refuses what the fold would mark, on both routes.** Where an operation declares a precondition, or its effect is `merge` and its type a schema, the client's pre-flight (`client.Preflight`, the SDK's own and the node's at the door of a verified write) reads the instance's state once from the state bucket and refuses with `ErrPreconditionFailed` or `ErrStateOffSchema` before anything is sent; the node answers a verified write with **`precondition-failed`** or **`state-off-schema`**, which the SDK maps back to the same sentinels. The judgement is a best effort against a moving state unless the writer carries the expected sequence; the fold is the judge either way, and a bare publish is never refused.
5. **The CLI.** `op create TYPE OP --schema FILE --effect merge|none [--precondition FILE]`; a changed precondition is announced as the rebuild it causes, a re-stated one is not; `op get` prints the precondition; `type create` and `type get` echo `(merge, precondition)`; `type init`'s skeleton carries a commented example. `instance apply` and `instance create` refuse out of turn or off schema with the reason. `instance history` gains a **MARK** column: a history read from its start is judged as the state is derived — the author gate of a store that requires verified authors, then the fold step under the current declarations, per path — and an operation the state did not take shows *invalid* with its reason; with `--from` the state before is unknown and no mark is claimed.
6. **The artifact and the suite.** `contract/sdk-contract.json` at 3.2.0: `precondition` on the type record's operation entry and on `type.define`'s request, the two decisions in the fold section with the rule stated, the two codes in the catalog and on `apply`. Golden fixtures in `fold.json`: a precondition met, one not met, one on an effect-`none` operation, a merge off schema (a required field dropped; a field the schema's `else` forbids), a precondition on `create` judged against the empty object. Live scenario `04-preconditions`: refused pre-flight on the direct route and through the node, a bare publish of each kept and marked, the state standing, the history keeping all four.

## Constitution check

Against `AGENTS.md` and `how-we-build`:

- **Nothing sits in the direct append path.** A bare publish lands as it always did; the pre-flight is the writer's own and the node's on `APPLY` only. The fold marks, never drops.
- **The wire contract is the thing under test, against real NATS.** The node test publishes bare records over the embedded server and reads the marks from the fold's log; the scenario runs over `up`.
- **The client surface is the artifact's.** The artifact, the Go parity test, the fixtures and the scenario change in this PR; the TypeScript SDK follows the artifact in `chronicle-js`, the next repo in the landing order.
- **The words are the user's.** *precondition* is the vocabulary's word (chronicle-hq decision 0059); *invalid* with its reason is what a user reads; *fold*, *marked* and *judge* stay in the record and the code's comments. `cli/vocabulary_test.go` checks the help.
- **Minimal build.** No transition table, no scripted effect, no opt-in flag on the post-state rule, no admission gate: one JSON Schema facet and the validator every SDK already runs.

## What it does not do

No change to `Op-Version`. No refusal on the direct path. No server-side guarantee that the pre-flight's state read is current: a writer that wants the refusal exact carries the expected sequence, and a stale judgement is caught by the fold. No mark on a history read with `--from`, and no persisted marks anywhere — a mark is derived on every read, like state. No change to which operations the rollup gate absorbs beyond judging the merge's result as the fold does.
