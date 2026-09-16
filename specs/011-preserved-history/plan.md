# Plan 011 — preserved history

## Layout

```
contract/metakeys.go       LogConfig.History; the history vocabulary
                           (HistoryCompactable, HistoryPreserved) and
                           NormalizeHistory — write-side strict, read-side
                           tolerant, the effects pattern repeated
contract/streams.go        LogStreamConfig takes the declaration; AllowRollup
                           false exactly when preserved
client/api.go              LogCreateRequest.History; LogOpt + WithHistory;
                           CreateLog goes variadic (backward-compatible)
internal/node/node.go      handleLogCreate validates the vocabulary
                           (bad-history), records it, passes it to the stream
internal/node/rollup.go    the log-level gate at the top of rollupThing —
                           one place, both triggers; KV get per attempt is
                           noise next to the replay it replaces
internal/cli/cli.go        log create --history; the usage line
contract/contract_test.go  the stream table under both declarations
internal/node/preserved_test.go  the 0019 contract; the two-log sweep
internal/cli/cli_test.go   the spine grows the preserved log and its named
                           decline
```

## Mechanics

- **The gate reads META per attempt.** History is immutable at creation,
  so no watcher is needed; a KV get per rollup attempt costs nothing
  next to the per-subject replay it short-circuits, and it keeps the
  gate correct even across node restarts with zero cached state.
- **Read-side tolerance is safe here** because the node's gate is
  manners, not the guarantee: an unreadable config reads as compactable,
  and if the log was truly preserved the stream's `AllowRollup: false`
  refuses the publish anyway — defense in depth, the server the last
  word, exactly the write path's posture.
- **`SaveVersion` needs no change**: the server refuses its rollup write
  on a preserved stream, and the SDK surfaces that refusal as the
  publish error it is (it is not a stale version — the caller's recourse
  is "this log does not do that", not "re-read and retry").

## Tests

All against a real embedded NATS server, race on.
`TestPreservedHistory`: the unknown value refused write-side; a fully
merge-covered thing declined with "preserved" in the reason; SaveVersion
refused by the stream; the trail intact (birth + op) after both.
`TestPreservedLogSurvivesTheSweep`: two logs of identical shape under a
50 ms timer — the compactable control compacts (the sweep is live), the
preserved log keeps its trail through the same sweeps. Contract:
`AllowRollup` false exactly when preserved, the rest of the table
standing. CLI spine: `--history preserved` at create, the verb's decline
printed with the declaration named.
