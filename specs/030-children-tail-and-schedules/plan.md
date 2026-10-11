# Plan 030 — a tail with children, and a store that allows schedules

## Layout

```
contract/sdk.go                 ContractVersion 2.1.0
contract/sdk-contract.json      version; store.create.request.schedules; history and tail sources
contract/metakeys.go            StoreConfig.Schedules
contract/streams.go             StoreStreamConfig(..., schedules): AllowMsgSchedules, AllowMsgTTL
contract/contract_test.go       the flags on and off; nothing else moves
client/api.go                   StoreCreateRequest.Schedules; WithSchedules
client/iter.go                  Children TailOpt; ops over several filter subjects; historyUnderTail
client/paths.go                 HistoryUnder
client/sdk_contract_test.go     the store.create example carries schedules
node/node.go                    the config and the stream carry the flag
cli/instances.go                instance history --children, the PATH column
cli/store.go                    store create --schedules; store get and list say it
conformance/scenarios/02-…      a children tail sees a child's create; a store with schedules is created
conformance/scenarios_test.go   children and schedules on the steps
specs/030-…/
```

## Mechanics

1. The contract and the artifact first, so the parity test drives the rest.
2. The client: `Children()` adds the `.>` filter beside the exact subject on
   the ordered consumer; `HistoryUnder` is the bounded read with both.
3. The node passes `Schedules` into the config record and the stream.
4. The CLI: the two flags, the `PATH` column, `store get`'s line.
5. The conformance suite: the step shape gains `children` and `schedules`;
   scenario 02 tails `invoice/inv-1` with children and creates
   `invoice/inv-1/comments/c-9`, expecting `create` to arrive; a store
   `ticks` is created with schedules.
6. `make check` green: fmt, tidy, build, race tests (the suite boots `up`),
   lint.
7. One PR, draft from the first push, labelled `work/59`, ready and merged
   after the chronicle-hq PR; then the release tag `v0.5.0` (decision 0017),
   whose archive carries the 2.1.0 artifact and suite; then chronicle-hq
   flips the four designs back to `implemented`.
