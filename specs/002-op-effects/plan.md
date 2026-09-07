# Plan 002 — op types declare their effect

## Layout

```
contract/metakeys.go       TypeSchema.Effect; EffectNone/EffectMerge;
                           NormalizeEffect, KnownEffect
contract/merge.go          MergePatch — RFC 7386, implemented once for
                           the fold and any reader wanting the same answer
contract/merge_test.go     the RFC's appendix-A table + vocabulary helpers
client/api.go              SchemaSetRequest.Effect; SetSchema carries it
internal/node/node.go      bad-effect refusal (write-side strictness);
                           rebuild on effect change
internal/node/store.go     recordSchema stores the normalized effect and
                           reports whether it changed (absent counts as
                           none, so a first declaration with merge counts)
internal/node/fold.go      per-log foldRun handles (idempotent stop that
                           waits for Closed()); applyEffect — the 0011
                           rules; casState — one CAS loop shared by
                           snapshot reset and merge; rebuildLog — stop,
                           purge, re-fold
internal/node/effects_test.go  merge/none/invalid/unborn/bad-effect and
                           the purge-and-refold, on real NATS
internal/cli/cli.go        schema set --effect
internal/cli/cli_test.go   the flag reaches the wire and state merges
```

## Mechanics

- **Write-side strictness, read-side tolerance**: `SCHEMA.SET` refuses
  effects outside this node's vocabulary (`bad-effect`); the fold treats
  an unknown recorded value as `none` with a warning, so a newer node's
  records never break an older fold.
- **Change detection is normalized**: absent and `""` both mean `none`,
  so only a real semantic change (including a *first* declaration of
  `merge` over history folded without it) triggers the rebuild.
- **The rebuild** stops the log's fold (waiting on `Closed()` so no
  straggler writes after the purge), purges every state key, and starts
  a fresh ordered consumer from the beginning — `DenyDelete` on the
  stream means the log itself is untouched; only derived state moves.
- **One CAS loop** (`casState`) serves both writers: snapshot resets
  create-if-absent, merges refuse to apply before any snapshot exists
  (pattern § 5.1 — the log is malformed for state until one appears),
  and both skip at-or-below the stored seq so redelivery stays
  idempotent.

## Tests

Contract: the RFC 7386 appendix table verbatim, plus vocabulary helper
edges. Node (embedded real NATS, race on): merge overwrites named fields
and keeps the rest; null deletes; `none` ops and schema-invalid ops move
nothing (marked, never dropped — and still land in the log); ops on
unborn subjects warn and take no effect; unknown effects are refused at
declaration; an effect change purges and re-folds the whole history
under the new declaration while replay shows the log untouched. CLI:
`--effect merge` round-trips and the merged state appears through the
state verb.
