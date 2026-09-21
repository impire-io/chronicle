# Plan 024 — the SDK contract

## Layout

```
contract/schema.go        CompileSchema (moved from client)
contract/judge.go         Decision, JudgeSnapshot, JudgeRecord (moved from
                          foldcore) and FoldStep — the one fold step
contract/errors.go        the error catalog: Code* constants, ErrorCatalog
contract/sdk.go           ContractVersion, the six shapes, the client defaults
contract/stream.go        Chron-Chunk / Chron-End, ChunkByteBudget, ChunkWriter
contract/logname.go       LogNamePattern, ThingTokenPattern, ReservedLogNames exported
contract/sdk-contract.json  the artifact
foldcore/foldcore.go      aliases onto contract; Pass.Fold drives FoldStep
client/iter.go            bucket scans as iterators; Tail, Watch, WatchDeclarations
client/read.go            List*, Replay, FoldTail return iter.Seq2
client/stream.go          Streamed[Item, Trailer], RequestStream: the client loop
client/api.go             the queries return *Streamed; offset gone; trailers
client/sdk_contract_test.go  the artifact's proof
index/{search,graph,semantic}  chunked responders through ChunkWriter
node/*, index/*           error codes as contract.Code*
cli/cli.go                iterators printed as they arrive; --json JSON lines
conformance/              fixtures/, scenarios/, conformance_test.go
.goreleaser.yaml          archive carries conformance/
AGENTS.md, README.md      the contract-changes-the-suite line; the sentences
```

## Mechanics

- **Bucket scans stream.** A list is a KV watch on the key pattern with
  `IgnoreDeletes`, consumed until the initial-values marker; keys-only
  scans add `MetaOnly`. Order is stream order — the bucket's — not sorted.
- **History is bounded at the observed head.** `Replay` and `FoldTail`
  keep the ordered consumer and the fetch cadence; they stop at the
  pending count read when they started. `Tail` runs the same consumer
  without the bound, `Live()` as `DeliverNew`.
- **The streamed reply loop** subscribes an inbox with the contract's
  pending limits, publishes the request with the inbox as reply, and
  reads with the stall as the per-message wait: a `Status` header of 503
  is no responder; micro error headers raise; `Chron-Chunk` is verified;
  `Chron-End` parses the trailer and closes.
- **Responders write chunks**: `ChunkWriter.Item` buffers marshalled
  items and flushes a chunk when the budget would overflow; `End`
  flushes and sends the trailer. The services keep their query
  functions, now yielding items to the writer instead of slicing.
- **The fold moves, the pass stays.** `FoldStep` is pure — resolution,
  state and op in; decision, detail, state and moved out. `Pass` keeps
  the frontier, idempotency and the sink.
- **The CLI prints per item** and closes a `query` with the trailer's
  total; `--json` writes one JSON object per line for collections.
