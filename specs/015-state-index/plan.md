# Plan 015 — the state index

## Layout

```
contract/metakeys.go     IndexKindState + the reserved StateIndexName;
                         StateFoldKey ("=fold" — "=" is KV-legal and
                         refused in thing tokens, so no collision) and
                         FoldWatermark
internal/foldcore/pass.go    Pass: the one fold state machine — resolve,
                         judge, per-thing frontier and state, sink; Seed
                         for checkpoints, Snapshot/Things for the fleet's
                         horizon and roster reads
internal/foldcore/foldcore.go  LogFingerprint: every type record's fold
                         fingerprint, sorted — the declaration watermark
internal/node/node.go    LOG.CREATE writes index.<log>.state
internal/node/index.go   DECLARE refuses the kind and the reserved name;
                         DELETE declines it; re-derivation skips it (the
                         state index places no workload — it rides the
                         node)
internal/node/fold.go    the fold on the Pass: sink is the KV-CAS write,
                         Track feeds the sweep, the watermark is stamped
                         at every fold start (a rebuild purges it with
                         the keys and re-stamps under the new
                         declarations)
internal/index/projection/   state-sourced passes fold through the Pass;
                         seedFromCheckpoint loads {seq, state} under a
                         matching watermark and consumes from just past
                         the oldest seeded seq; backlog is measured from
                         the consumer, not the stream
internal/workloads/      the fleet fold on the Pass with a by-family
                         resolver; horizon and the roster scan read the
                         Pass; the fleet log declares its state index too
```

## Mechanics

- **One state machine, native lifecycles.** The Pass owns what was
  drifting in three copies — resolve → judge → merge, the frontier
  guard, snapshot validation, marking — while consumers, rebuild
  strategies, and swap points stay with their owners: converging those
  would have moved the scheduler's roster into foldcore for no present
  need. Two frontiers ride each thing: the last op seen (idempotency)
  and the last state move (the fleet's knowledge horizon — its old
  meaning, kept deliberately).
- **The node folds in memory now, like every other pass**, and the
  bucket write is a plain CAS-if-newer sink. Replay from sequence 1 at
  every fold start makes memory and bucket equivalent by determinism;
  racing writers keep being harmless by construction.
- **The checkpoint is conservative.** Seeding happens only under a
  byte-equal watermark and a clean read of every entry; anything less
  replays whole. The consume-from point is the oldest seeded seq plus
  one — per-thing guards skip what the seeds cover, and everything the
  bucket does not hold contributed nothing to state under the same
  declarations. Post-compaction logs barely need it (roll-up already
  snapshots in-stream); uncompacted and preserved logs are where it
  pays.
- **No workload, no query endpoint.** The state kind's read surface is
  the bucket plus the exactness recipe; the supervisor never hears of
  it.

## Tests

`internal/node/state_index_test.go`: the declaration born with the log,
the DECLARE/DELETE refusal matrix, the watermark stamped and re-stamped
across a declaration change. `internal/index/search`'s
`TestSearchBootsFromStateCheckpoint`: a checkpoint boot answers exactly
as a replay would (and says so in its log line); a tampered watermark
voids the shortcut and the full replay answers the same. The whole
existing suite — node, rollup, aspects, preserved, search, semantic,
graph, fleet, CLI — runs on the shared pass unchanged: the strongest
behavior-parity evidence the refactor has.
