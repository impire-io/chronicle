# Plan 003 — compaction

## Layout

```
client/api.go              ThingRollupSubject; ThingRollup{Request,Response};
                           RollupThing — the verb, spoken
client/ops.go              SaveVersion + ErrStaleVersion — the app-initiated
                           rollup, retry-safe like CreateThing
internal/node/node.go      Config.RollupEvery (0 → 1 h default); the
                           THING.ROLLUP endpoint; the timer goroutine
internal/node/rollup.go    rollupThing — replay, judge under the gate, fold
                           in memory, publish with the guard; rollupActive —
                           the timer's sweep; the per-log mutex
internal/node/fold.go      the fold records active subjects (swap-and-clear);
                           rebuildLog takes the per-log mutex
internal/node/store.go     requireRole goes variadic (admin-or-writer)
internal/cli/cli.go        chronicle thing rollup
internal/node/rollup_test.go  SaveVersion (compact, stale, retry); the verb
                           (covered, the three vetoes, nothing-to-compact,
                           roles); the timer (compacts covered, leaves vetoed)
internal/node/node_test.go startNodeWith — startNode with a Config
internal/cli/cli_test.go   thing rollup: one compacted, one vetoed
```

## Mechanics

- **One routine, two triggers.** `rollupThing` is self-contained: it
  replays the exact subject through its own ordered consumer, so it
  depends on neither the state bucket's freshness nor the fold's
  position. The timer and the verb both call it; the
  expected-sequence guard (set to the last replayed seq) settles every
  race — another rollup, an SDK save, a new op — server-side.
- **The gate is judged at replay time** against the current META
  declarations, the same "latest declaration wins" the fold obeys.
  The judge mirrors the fold's rules exactly; where the fold warns and
  moves nothing, the judge vetoes — what the fold could not capture,
  the node must not destroy.
- **Veto is an answer.** The verb returns `{rolled: false, reason}`;
  the timer logs it at debug. Losing the publish race is the same
  shape: skip, nothing to clean up (pattern § 5.2).
- **Activity is the fold's knowledge.** `fold.apply` already sees
  every ops-family message; it records the thing into a per-fold set
  the timer swaps out. A rebuild or restart re-folds the stream and
  re-marks everything — the next sweep is a full one, by design.
- **The per-log mutex** serializes `rollupThing` against `rebuildLog`
  so a rollup never publishes state computed while declarations are
  being re-folded. It lives on the node, keyed by log, and survives
  fold restarts.
- **SaveVersion mirrors CreateThing's recovery**: the guard fires
  before dedup, so a retried save surfaces wrong-last-sequence; read
  the subject's last op and compare IDs to tell "my save landed" from
  "the log moved" (`ErrStaleVersion`).

## Tests

All against a real embedded NATS server, race on. SaveVersion:
compacts to one snapshot (replay shows it; the fold converges the
bucket to it), carries the frontier, appends keep working after; a
stale `upTo` returns `ErrStaleVersion` and replay is untouched; a
retried save with a pinned op ID reports the original landing. The
verb: a covered history compacts and merges correctly; an
`effect: none` op, an unknown type, and a marked (schema-invalid) op
each veto with their reason; a bare birth is "nothing to compact"; a
writer may trigger, a reader is refused; unknown log and thing are
named errors. The timer (`RollupEvery` in the tens of milliseconds):
a covered subject compacts without any verb call; a vetoed subject's
history stays. CLI: `thing rollup` prints the compaction and the veto.
