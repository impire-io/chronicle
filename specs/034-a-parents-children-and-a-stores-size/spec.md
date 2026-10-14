# Spec 034 — a parent's children in order, with their types; a store's size

**Work ID:** `chronicle-60` (tracker item chronicle-60, a task)
**Design:** `chronicle-hq` @ `a1449cc` —
[`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md)
§ instances (`instance list --in PATH`) and § the grammar (`store get`),
decision [`0045`](../../../chronicle-hq/03-DECISIONS/0045-the-cli-is-noun-verb-and-a-list-is-a-list.md)
(a list is a list), and the SDK artifact's `list.instances` item, whose
`type` is "the type its path names".
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

Three nits seen on the chronicle-agents spike (tracker chronicle-60),
each a sentence saying less than it knows.

- **A parent's children read in sequence order.** `instance list --in
  PATH` sorted by path unless `--sort seq` was given, and a parent's
  children are read in the order they were written far more often than
  alphabetically. Under `--in` the default sort is now `seq`; `--sort`
  still overrides it, and every other listing keeps `path` as its
  default. The flag's help says so.
- **The TYPE column names the child's type.** Under a parent the column
  showed the child segment's name (`comments`) rather than the child's
  type (`comment`), because the client consulted the parent's record
  only when `--type` narrowed the listing. The client now resolves the
  parent on every listing under it and names each child's type from
  the parent's children map — the artifact's "the type its path
  names" — falling back to the segment's name only when the parent is
  untyped or the name is undeclared, the grammar's own answer.
- **`store get` says the size.** It showed the history policy and the
  description, the byte budget only when the store set its own, and the
  bytes used never. It now reads the store's stream and prints the
  bytes used against the budget in effect — the default budget named
  as such when the store's settings set none — and the JSON form
  carries `bytes` and `max_bytes`. `client.StoreUsage` is the read,
  beside `GetStore`: data at rest read through JetStream, as the
  artifact's reads are.

## Out of scope

- **The TypeScript SDK.** `chronicle-js` restates the listing and names
  a child by its segment the same way; it is its own repo's fix, filed
  against it.
- **The artifact.** `list.instances` already describes `type` as the
  type the path names — the Go client now honours it — and the single
  reads (`GetStore`, `GetType`, now `StoreUsage`) are not interactions
  the artifact enumerates. No contract version bump, no conformance
  change.

## Requirements

- **FR-01 Order.** `instance list --in PATH` without `--sort` lists the
  parent's direct children by `seq` ascending; with `--sort F` it sorts
  by `F`; without `--in` the default stays `path`.
- **FR-02 Type.** Under `--in`, each row's `type` is the type the
  parent's children map declares for the child's segment name, in the
  table and in `--output jsonl`; an undeclared segment or an untyped
  parent yields the segment name.
- **FR-03 Size.** `store get NAME` prints `size: <bytes> of <max> bytes
  used`, suffixed ` (the default budget)` when the store's own
  `max_bytes` is zero, and its JSON carries `bytes` and `max_bytes` as
  the stream reports them; a name no store holds answers `ErrNoStore`.
- **FR-04 The gate (quality).** `make check` green, race on; the
  vocabulary test passes on the new help text.
