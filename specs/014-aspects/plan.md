# Plan 014 — aspects

Rides spec 013's machinery — the resolver landed complete there, so this
spec's own surface is the marking behavior, the pre-flight refusal, and
the roll-up gate order.

## Layout

```
contract/resolve.go      ResolvedUndeclared: a pair's segment missing
                         from its parent type's aspects map — with the
                         missing-target case (segment declared, aspect
                         type undefined) reading the same way
internal/node/fold.go    resolution precedes the snapshot branch, so a
                         marked subject's birth moves nothing; marked =
                         no state write, log untouched
internal/index/projection/  state-sourced passes drop marked subjects;
                         ops-sourced passes are untouched — declarations
                         shape state, never history's visibility (0020)
internal/node/rollup.go  gate order: 0019 log declaration (unchanged) →
                         resolved type's history (preserved declines with
                         the type named; compactable absorbs — captureTyped
                         has no per-op veto) → effect-coverage veto for
                         untyped subjects only (captureUntyped); a marked
                         subject declines outright; absorption still needs
                         one valid snapshot to fold from
client/ops.go            preflightSnapshot refuses ErrUndeclaredAspect on
                         birth and save; preflightAppend refuses it on
                         appends to a marked subject
```

## Mechanics

- **Marking is subject-whole and retroactive both ways.** The fold
  resolves before it looks at the op, so snapshots on an undeclared
  aspect are marked too. Redemption and re-marking are the ordinary
  rebuild: the aspects map is part of the fold fingerprint (spec 013),
  so an aspects change purges and re-folds — the same path an effect
  change takes, no aspect-specific machinery.
- **Absorption has one floor.** A typed compactable thing compacts past
  none-effects, unknown ops, and marked junk (0022 § 5 — the veto is
  retired there), but never without a valid snapshot in the replay: a
  rollup must not replace history with a state that never existed.
- **Discovery is not built.** Readers walk the aspects map themselves;
  there is no scan, no listing verb, no parent-child table — the address
  is the relationship.

## Tests

`internal/node/aspects_test.go` runs 0022 end to end: independent folds
for parent and aspect, multi-level chains, the parent compacting while
its preserved aspect declines with the type named, the SDK refusing an
undeclared aspect, a raw publish marked out of state while the log keeps
it, and the declaration change redeeming the marked subject retroactively.
The roll-up suite (`rollup_test.go`) pins the inverted semantics: typed
compactable things absorb none-effects and marked junk; untyped subjects
keep 0011 § 4's veto, on the verb and across timer sweeps.
