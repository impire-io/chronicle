# Spec 024 — the SDK contract

**Work ID:** `012-the-sdk-contract` (chronicle-hq research 012, graduated)
**Design:** `chronicle-hq` @ `9ce9669` (hq PR #36, merged) —
[`03-DECISIONS/0036-the-sdk-contract.md`](../../../chronicle-hq/03-DECISIONS/0036-the-sdk-contract.md),
[`02-DESIGN/12-the-sdk-contract.md`](../../../chronicle-hq/02-DESIGN/12-the-sdk-contract.md),
and the amended [`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md)
and [`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md).
**Status:** implemented ([plan.md](plan.md)) — on this repo's main since
2026-09-22 (PR #32); the designs read `implemented` (hq PR #37).

## What this delivers

The Go client takes the shape every SDK will restate, and the repo gains
the two things a second SDK is checked against: the contract artifact and
the conformance suite.

- **Collections are iterators; single state is a reply.** Every
  collection read returns an `iter.Seq2[T, error]` — logs, types,
  indexes, members, things (bucket scans, streamed one key at a time),
  and history (`Replay`, `FoldTail`: the ordered consumer). Single values
  — a thing's state, a type record, an index declaration, a verb's reply
  — are unchanged.
- **The live surface.** `Tail` (a log or one thing, from a sequence,
  from the start, or live), `Watch` (a thing's state: current value then
  every change), `WatchDeclarations` (a log's type records and index
  declarations). All fed by JetStream — ordered consumers and KV watches
  — never a core subscription.
- **The index queries are streamed replies.** Search, graph neighbors and
  walk, semantic: one request, chunks of items under a byte budget
  numbered by `Chron-Chunk`, a trailer marked `Chron-End` carrying the
  item count and the kind's totals (`total`; `unembedded` for semantic;
  `depth_capped`, `truncated` for a walk). `limit` is an optional cap —
  absent, every match streams — and `offset` is retired. An error
  mid-stream is a message with the micro error headers; the client
  raises after the items already yielded. No resume cursor.
- **The error catalog.** Every code the node and the indexers return is a
  constant in `contract` and a catalogued entry with its meaning; the
  services speak the constants.
- **The fold rules are the contract's.** `contract.FoldStep` — resolve,
  judge, merge or reset, mark — is the one fold step; `foldcore.Pass`
  drives it, and an SDK performing the exactness recipe applies it.
  `CompileSchema` moves to `contract` with it.
- **The contract artifact.** `contract/sdk-contract.json`: the contract
  version, the six shapes, every interaction with its subject, shape,
  role, schemas and error codes, the operation record, the grammars, the
  error catalog, the client defaults. A test proves the Go constants,
  patterns, subjects, codes and defaults equal the artifact's, and that
  the Go request types satisfy their schemas.
- **The conformance suite.** `conformance/`: golden fixtures (names,
  derivations, headers, resolution, merge patch, fold, the guard-retry
  judgement) and live scenarios against `chronicle up`, with the Go
  client as the first runner. The release archive carries it.
- **The CLI follows.** Collections print as they arrive; `--json` emits
  JSON lines for a collection and one object for a single value; `query`
  loses `--offset` and its total closes the listing.

## Out of scope

- **`chronicle-service`'s two `ListMembers` calls** — it pins
  `chronicle` v0.2.0 and adapts at its next bump (a follow-up item).
- **Batch publish and scatter/gather** in the client — described in the
  artifact, built when a consumer exists (0036 § 3).
- **The TypeScript and Python SDKs** and their repos — after this lands.
- **A resume cursor on streamed replies** (0036 § 4).

## Requirements

- **FR-01 Iterators.** `ListLogs`, `ListTypes`, `ListIndexes`,
  `ListMembers`, `ListThings`, `Replay`, `FoldTail` return
  `iter.Seq2[T, error]`; a bucket scan is a KV watch consumed to its
  initial marker, streamed in stream order; history is the ordered
  consumer bounded at the head observed when it started. Cancelling the
  caller's context ends the iterator and releases the source.
- **FR-02 The live surface.** `Tail(ctx, log, thing, opts...)` yields
  ops with their sequence, from the start by default, `After(seq)` or
  `Live()` by option; an empty thing tails the whole log. `Watch(ctx,
  log, thing)` yields `{seq, state}`, the current value first.
  `WatchDeclarations(ctx, log)` yields each type record and index
  declaration as it stands and as it changes.
- **FR-03 Streamed queries.** `QueryIndex`, `QuerySemantic`,
  `GraphNeighbors`, `GraphWalk` return a `*Streamed[Item, Trailer]`:
  `Items()` is the iterator, `Trailer()` is readable once it ends. The
  client verifies `Chron-Chunk` increases by one (a gap is an error),
  treats a stall longer than `contract.StreamStall` as an error, raises
  a catalogued `ServiceError` on an error message, and reports no
  responder as a typed error naming the subject. A `Status: 503` reply
  is no responder.
- **FR-04 Chunked responders.** The three index services answer through
  `contract.ChunkWriter`: items under `contract.ChunkByteBudget` per
  chunk, the trailer last. `limit` caps items; zero or absent means all.
- **FR-05 The catalog.** `contract.Code*` constants and
  `contract.ErrorCatalog`; no string-literal code remains in the node or
  the indexers.
- **FR-06 The fold step.** `contract.FoldStep(res, state, op)` returns
  the decision, the detail, the new state and whether it moved;
  `foldcore.Pass.Fold` applies it; `foldcore` keeps its names as aliases
  and no longer imports `client`.
- **FR-07 The artifact and its proof.** `contract/sdk-contract.json`
  with the fields above; `client/sdk_contract_test.go` fails when any
  constant, pattern, subject, code, default, shape or version diverges,
  or when a Go request type does not satisfy its schema.
- **FR-08 The suite.** `conformance/fixtures/<family>.json` and
  `conformance/scenarios/*.json`, with `conformance/conformance_test.go`
  running both against the Go client and `up`; the goreleaser archive
  includes `conformance/`.
- **FR-09 The CLI.** Every collection sentence prints as it arrives and
  takes `--json` (JSON lines); `get` and `type inspect` take `--json`
  (one object); `query` takes `--limit` only; `AGENTS.md` states that a
  contract change changes the suite in the same PR.
- **NFR** `make check` green: fmt, tidy, build, race tests, lint; the
  wire runs against real NATS in every test.
