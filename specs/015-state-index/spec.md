# Spec 015 — the state index

**Work ID:** `007-fewer-concepts` (chronicle-hq research 007)
**Design:** `chronicle-hq` @ `17113e1` (hq PR #8 — re-cite on merge) —
[`03-DECISIONS/0023-state-is-a-declared-index.md`](../../../chronicle-hq/03-DECISIONS/0023-state-is-a-declared-index.md)
and [`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md)
§ the declaration, § the state kind, [`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md)
§ the state buckets.
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

State stops being the one read view outside the framework. Today
`STATE_<LOG>` has no declaration, no kind, and its materialization is a
second fold implementation the node carries beside the indexers' shared
projection spine (with a third hand-written copy in the fleet service).

- **The declaration `index.<log>.state`, kind `state`** — written by
  the node at `LOG.CREATE`, materialized by the node into `STATE_<LOG>`
  exactly as today (value `{seq, state}`, key the subject tail, per-key
  history on, revision-CAS). `INDEX.DECLARE` refuses the kind — and the
  reserved index name `state` — from callers; `INDEX.DELETE` declines
  it with the reason named while the log exists: the exactness recipe,
  0018's guard values, and roll-up are its consumers.
- **One projection spine.** The node's fold, the indexers' spine, and
  the fleet service's copy converge on one implementation with
  pluggable engines — the state engine is the KV-CAS writer, the others
  stay in-memory. One rebuild rule: build-and-swap where the store is
  memory, purge-in-place where the store is the KV. The active-set feed
  to the roll-up sweep and the log-mutex serialization stay
  node-internal.
- **Checkpoint bootstrap.** A state-sourced indexer may boot from the
  state index — read each subject's `{seq, state}`, consume from
  `seq + 1` — instead of re-folding from sequence 1, abandoned for a
  full replay when the log's type declarations changed since the
  checkpoint was written (the same suspicion rule as everywhere).
  Ops-sourced passes gain nothing: their documents are per-op.

## Out of scope

- **A `chronicle-index-state` placement** — the state index rides the
  node; moving it is a scheduler-time decision.
- **A query endpoint for the state kind** — its read surface is the
  bucket plus the exactness recipe.
- **Persisted search/vector indexes** — the checkpoint shortens the
  replay; it does not persist engines.

## Requirements

- **FR-01 The declaration.** `LOG.CREATE` writes `index.<log>.state`
  (`{kind: "state"}`) beside the bucket it already creates; DECLARE
  refuses kind `state` and index name `state` write-side with the rule
  named; DELETE declines `index.<log>.state` with the reason. Log
  destruction removes it with everything else.
- **FR-02 The spine.** The fold rules exist once: a shared per-op
  state machine (`foldcore.Pass` — resolve, judge, per-thing frontier,
  sink) drives the node's fold (sink: the KV-CAS write; Track: the
  active-set feed), the indexers' state-sourced passes (sink: the
  engine upsert), and the fleet's fold (its own family resolver; sink:
  the STATE_FLEET mirror plus the scan kick). Lifecycles stay native —
  build-and-swap where the store is memory, purge-in-place where the
  store is the KV: exactly the two rebuild strategies the decision
  names. Behavior holds: same bucket writes, same purge-and-refold on
  declaration change, roll-up still fed, the fleet's knowledge horizon
  unchanged (the pass tracks the frontier and the last state move
  separately).
- **FR-03 The checkpoint.** The state materialization records, beside
  its keys, a fold watermark naming the type-declaration revisions it
  was derived under; a state-sourced indexer boots from `{seq, state}`
  when the watermark matches the current declarations and replays from
  sequence 1 when it does not (or when absent). The watermark's key
  lives outside the thing-tail namespace so no customer key can
  collide with it.
- **FR-04 The gate.** `make check` green, race on, real embedded
  NATS — including: the declaration present after `LOG.CREATE` and
  refused to DECLARE/DELETE; state materialization byte-identical
  through the spine change (existing state/fold/rollup tests stand);
  an indexer booting from the checkpoint answers identically to one
  booting by replay; a declaration change since the checkpoint forces
  the full replay; the fleet fold still folds `STATE_FLEET`.
