# Spec 030 — a tail with children, and a store that allows schedules

**Work ID:** `59` (tracker chronicle-59; closes chronicle-61 in the same landing)
**Design:** `chronicle-hq` @ `1ef9636` (hq PR #70) —
[`03-DECISIONS/0049-a-tail-may-include-an-instances-children.md`](../../../chronicle-hq/03-DECISIONS/0049-a-tail-may-include-an-instances-children.md),
[`03-DECISIONS/0050-a-store-may-allow-schedules.md`](../../../chronicle-hq/03-DECISIONS/0050-a-store-may-allow-schedules.md),
and the amended designs [`02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md) § Streams,
[`03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md) § Store configuration,
[`07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md) (`store create`, `instance history`),
[`12-the-sdk-contract.md`](../../../chronicle-hq/02-DESIGN/12-the-sdk-contract.md) § Subscribe.
**Status:** in progress ([plan.md](plan.md)).

## What this delivers

Two additive growths of the account plane, asked for by chronicle-agents
and decided in chronicle-hq, landing as contract 2.1.0:

- **A tail with children (0049).** `client.Children()` on `Tail`: the
  ordered consumer filters to the instance's exact subject and everything
  under it, in one stream order; `client.HistoryUnder` is the bounded form.
  `chronicle instance history PATH --children [--follow]` prints the
  instance's and its children's operations with a `PATH` column. The
  artifact's `history` and `tail` entries describe the option; a live
  scenario tails a parent with children and sees a child's create arrive.
- **A store that allows schedules (0050).** `store.create` takes
  `schedules`; the store configuration records it; the stream is created
  with `AllowMsgSchedules` and `AllowMsgTTL` — and `DiscardOld`, the one
  setting the server forces beside them; `chronicle store create NAME
  --schedules` and `store get` say it; `client.WithSchedules()`. A contract
  test proves the flags and that nothing else moves; a live scenario
  creates such a store.
- **Contract 2.1.0**, additive, in `contract/sdk.go` and the artifact; the
  Go contract test proves they agree.

## What it does not do

No change to the wire's subjects or headers. No `schedule` verb or type of
chronicle's: a schedule is one headered publish any client makes, a tick is
the server's op. No default change: a tail of one instance means that
instance unless `Children()` is asked for; a store takes no schedules
unless created with them. `chronicle-js` stays at 2.0.0 until it moves.
