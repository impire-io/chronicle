# Plan 009 — the semantic index kind

1. **Contract**: `IndexKindSemantic`, `SemanticConfig{model?, fields?}`
   + `ParseSemanticConfig` (strict), the query payload and reply with
   the unembedded count, workload kind `index-semantic`.
2. **`internal/index/semantic`**: the provider client (OpenAI-compatible
   `/embeddings`, startup probe), the chunker (byte budget, per-field),
   the run (queued chunks → embed worker → chunk vectors; cosine +
   best-chunk queries; unembedded accounting), the service
   (declaration read, projection, fold-and-embed readiness gate, query
   endpoint).
3. **Backend seam**: `Supports(kind)` on `Backend`; provider config on
   `InProcess` and `Microsandbox` (key staged beside the creds for
   msb; `--embedding-key-file` on the workload binary); executor
   delegates `supports` to its backend.
4. **Composition + CLI flags**: `chronicle up` and `chronicle-executor`
   grow `--embedding-url/--embedding-model` (+ key env); node DECLARE
   validates semantic config; bridge maps the kind.
5. **SDK + CLI**: `QuerySemantic`; `chronicle semantic query`.
6. **Tests**: chunker and cosine units; wire test with an in-test HTTP
   provider (fold → embed → query → unembedded when the provider
   fails); fleet-level declared-semantic-serves with the test provider;
   the unprovisioned fleet leaves a semantic declaration unschedulable.
7. **Gate; live read** against local Ollama (`nomic-embed-text`);
   after merge: flip `05-indexes.md` to implemented.
