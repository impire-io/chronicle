# Plan 031 — a full-history store refuses purge

## Layout

```
contract/streams.go        StoreStreamConfig sets DenyPurge exactly when the
                           history is full; the doc comment names purge
                           beside rollup
contract/contract_test.go  the table under both declarations: DenyPurge off
                           on a compactable store, on with full history
node/preserved_test.go     TestPreservedHistory also attempts a subject purge
                           and a whole-stream purge against the embedded
                           server and expects the server's refusal (10110);
                           the trail check that follows covers both attempts
```

## Mechanics

- **Exactly when rollup is refused.** nats-server 2.14 rejects a stream
  that denies purge and allows rollup ("roll-ups require the purge
  permission"), so `DenyPurge` is `NormalizeHistory(history) ==
  HistoryFull` — the complement of `AllowRollup` — and never a default.
- **No version bump.** `contract/sdk-contract.json` describes verbs,
  headers and shapes, not stream settings; the conformance suite reads
  nothing of the table. `ContractVersion` stays at 2.1.0.
- **Existing streams are the operator's.** A config update may turn
  `DenyPurge` on and never off; the node keeps creating, never updating.

## Tests

`TestLogStreamConfig`: `DenyPurge` false on the default and the explicit
compactable declaration, true with full history. `TestPreservedHistory`
(real embedded NATS, race on): after the rollup refusals already proven, a
purge filtered to the store's subjects and a purge of the whole stream
both fail with the server's `10110`, and the trail still holds birth plus
the op.
