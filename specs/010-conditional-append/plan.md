# Plan 010 — conditional append

## Layout

```
client/ops.go              WithExpectedSeq + expectedSeq (tri-state pointer);
                           ErrThingMoved; the guard header on Append; the
                           shared guardRefused/ownOpLanded recovery, replacing
                           the duplicated tails in CreateThing and SaveVersion;
                           both non-append verbs refuse the misplaced option
internal/cli/cli.go        --expect-seq on append (Int64, -1 means unguarded);
                           the usage line
internal/node/guard_test.go  the 0018 contract against a real server: lands at
                           head, ErrThingMoved on reuse, retry recovery, the
                           exactly-one-winner race, unguarded unchanged,
                           guard 0, misplaced-option refusals
internal/cli/cli_test.go   the spine grows the guarded append and the stale
                           refusal, head parsed from the append output
```

## Mechanics

- **One option, one header.** `WithExpectedSeq` stores a pointer so
  absent and `0` stay distinct — `0` is the empty-subject guard, exactly
  birth's. `Append` sets the header only when armed; the publish path is
  otherwise untouched, and an unguarded append cannot hit the new error
  mapping (`o.expectedSeq != nil` gates it).
- **The recovery is now one helper.** `guardRefused(err)` names the
  wrong-last-sequence refusal; `ownOpLanded(ctx, log, subject, opID)`
  reads the subject's last op and reports whether the refusal was
  dedup's echo of our own landing. `CreateThing`, `SaveVersion`, and
  `Append` share both, each keeping its own sentinel and message.
- **The server stays the only arbiter.** The SDK's per-subject in-flight
  lock still only orders this process's publishes; the race test proves
  the exactly-one-winner property comes from the guard, not the lock.

## Tests

All against a real embedded NATS server, race on. `TestGuardedAppend`
(internal/node): a guarded append at the observed head lands; the same
guard re-used is `ErrThingMoved`; a retried guarded append with a pinned
op ID reports the original landing; two goroutines guarded at one head —
exactly one wins, the loser typed; an unguarded append still lands over
any history; guard `0` on an occupied subject is `ErrThingMoved`; birth
and save refuse the option. CLI spine: append prints the head, a guarded
append at it lands, the same guard re-used surfaces "moved past the
guard".
