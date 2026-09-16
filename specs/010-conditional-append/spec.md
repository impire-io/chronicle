# Spec 010 — conditional append

**Work ID:** `chronicle-10` (tracker item chronicle-10)
**Design:** `chronicle-hq` @ `f5ffea0` —
[`03-DECISIONS/0018-append-may-carry-an-expected-sequence-guard.md`](../../../chronicle-hq/03-DECISIONS/0018-append-may-carry-an-expected-sequence-guard.md)
and [`02-DESIGN/02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md)
§ the operation record (the opt-in guard, bullet added by 0018) and
§ the write path (division of duties).
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

The plain `Append` learns the guard every other write already carries:
birth stamps `Nats-Expected-Last-Subject-Sequence: 0`, `SaveVersion`
guards at `upTo`, the fleet's custody writes are guard-arbitrated — and
until now a tenant's ordinary append could not ask for the same
arbitration. A domain that must read-validate-append (every HITS write;
any refuse-before-record tenant) had no way to say "only if nothing
landed in between".

- **`client.WithExpectedSeq(seq)`** arms one append with the guard: the
  publish carries the header, JetStream enforces CAS per exact subject,
  and nothing new sits on the write path. Unset, `Append` is exactly what
  it was — unguarded stays the default and the common case.
- **A guard miss is `ErrThingMoved`** — the third sentinel beside
  `ErrThingExists` and `ErrStaleVersion`: re-read, re-validate, retry.
  The guard fires before dedup, so a retried guarded append surfaces the
  same refusal; the recovery (read the subject's last op, compare op IDs,
  report the original landing) is the one birth and save already do —
  now extracted into a shared helper instead of a third copy.
- **The guard value is the exactness recipe's yield**: `State`'s `seq`,
  advanced by `FoldTail`. No new read surface.
- **`chronicle append … --expect-seq N`** speaks it from the CLI;
  absent means unguarded.

## Out of scope

- **Guards on other verbs.** Birth guards at 0 by definition and a save
  guards at `upTo`; passing `WithExpectedSeq` to either is refused
  loudly rather than silently ignored.
- **Retry loops in the SDK.** The refusal tells the caller to re-read
  and re-validate; only the caller knows how. The SDK never re-reads
  state on the writer's behalf.
- **Wire or server changes.** The header, its enforcement, and its
  per-exact-subject scope were already the contract's.

## Requirements

- **FR-01 The option.** `WithExpectedSeq(seq uint64)` on `Append` sets
  `Nats-Expected-Last-Subject-Sequence: seq`. Tri-state: absent means
  no header; `0` is meaningful (the empty-subject guard) and behaves as
  any other value. `CreateThing` and `SaveVersion` refuse the option
  with an error naming their own guard.
- **FR-02 The refusal.** A server wrong-last-sequence on a guarded
  append maps to `ErrThingMoved` — unless the subject's last op is this
  very op, in which case the earlier attempt landed and the append
  reports it (retry-safe, same recovery as birth and save, one shared
  helper for all three).
- **FR-03 The CLI.** `chronicle append` takes `--expect-seq N`
  (unguarded when absent) and surfaces the sentinel's message on a
  miss.
- **FR-04 The gate.** `make check` green, race detector on, proven
  against a real embedded NATS server — including: a guarded append at
  the observed head lands; the same guard re-used is `ErrThingMoved`
  and the log is untouched; a retried guarded append with a pinned op
  ID reports the original landing; two racing guarded appends at one
  head produce exactly one winner; unguarded appends are unchanged;
  the misplaced option refuses on birth and save.
