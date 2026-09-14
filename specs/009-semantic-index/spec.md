# Spec 009 — the semantic index kind

**Work ID:** `chronicle-6` (tracker item chronicle-6)
**Design:** `chronicle-hq` @ `7f5f9c0` —
[`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md)
(§ the semantic kind) and
[`03-DECISIONS/0016-the-semantic-kind.md`](../../../chronicle-hq/03-DECISIONS/0016-the-semantic-kind.md)
(the provider deviation, defended).
**Status:** in progress on this branch ([plan.md](plan.md)).

## What this delivers

The third and last of the mission's index kinds; landing it flips
`05-indexes.md` to implemented.

- **The provider, as 0016 custody-splits it**: endpoint, key, and
  default model are **install configuration** — flags on
  `chronicle up` and `chronicle-executor`
  (`--embedding-url`, `--embedding-model`, key via
  `CHRONICLE_EMBEDDING_API_KEY`) — carried by the executor and handed to
  semantic placements; the key travels the workload secret channel (a
  staged file beside the service creds on the microsandbox backend, an
  in-memory handoff on backend zero), never env, never META. The
  declaration config is `{model?, fields?}` with install defaults.
  **An executor without a provider does not bid for semantic
  workloads** — the `Backend` seam gains `Supports(kind)`, so an
  unprovisioned fleet answers a semantic declaration with an
  unschedulable record, honestly, instead of a placement that cannot
  work.
- **`chronicle-index-semantic`** on the projection spine, with the fold
  and the embedding as the separate stages 0016 demands: `Upsert`
  extracts and chunks the selected fields (every string field by
  default, chunked to a byte budget with a default the operator can
  correct per model) and queues them; an embed worker batches queued
  things to the provider and stores chunk vectors; the fold never waits.
  **Readiness is fold *and* initial embed**: the query endpoint
  registers only after both, so a provider down at boot is a failed
  placement start — the executor's budget and the record say so.
  Live-tail failures mark things unembedded and retry with backoff;
  **every reply carries the unembedded count**.
- **The store**: in-memory chunk vectors, brute-force cosine per log,
  best-chunk scoring with the field carried into the hit — rebuilt (and
  re-embedded) by replay; persisted vectors wait on a measured bill.
- **The query contract**: `{text, limit, offset}` →
  `{hits: [{thing, score, field}], total, unembedded}` on the standard
  subject; SDK `QuerySemantic`; CLI `chronicle semantic query`.
- **The provider client is tested against a real HTTP server** — an
  in-test OpenAI-compatible `/embeddings` endpoint serving
  deterministic vectors — so the wire tests prove the fold-embed-query
  pipeline and the unembedded accounting with no external dependency
  and no skips; the live read runs against local Ollama
  (`nomic-embed-text`), keeping CI provider-free exactly as 0016
  requires.

## Out of scope

Per 0016: an embedded model runtime; persisted vectors and
approximate-NN (named triggers); provider config in declarations.
Re-ranking, hybrid search, and score fusion with the search kind — no
consumer. Per-tenant provider overrides (the operator owns the
substrate). Token-exact windowing (the chunk budget is bytes with a
correctable default; a tokenizer per model is weight nothing carries
yet).
