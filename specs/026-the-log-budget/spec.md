# Spec 026 — a log's byte budget follows the account's per-stream cap

**Work ID:** `chronicle-48` (tracker item; lands after chronicle-hq #46,
before chronicle-service)
**Design:** `chronicle-hq` decision
[`0039`](../../../chronicle-hq/03-DECISIONS/0039-a-logs-byte-budget-comes-from-the-accounts-plan.md),
amending [`02-DESIGN/02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md)
(the byte-budget row).
**Status:** implemented on this branch.

## What this delivers

NATS reserves a file stream's `MaxBytes` against the server's store when
the stream is created. A log's stream defaulted to 1 GiB whatever the
account, so an account whose limits allow less could not create a log at
all, and a host of small accounts ran out of promises long before bytes.

1. **`contract.LogBudget(requested, accountCap)`** — the creator's override
   when it names one; otherwise `DefaultMaxBytes`, or the account's cap
   when that is smaller. A cap of zero or less is none.
2. **`log.create` reads the cap from JetStream's own account
   information** (`storage_max_stream_bytes`) — plain NATS, any auth mode —
   and creates the stream with `LogBudget`'s answer. An open install with
   no cap keeps 1 GiB.
3. **An override above the cap is refused with `bad-request` before the
   META claim**, so a refused name stays free; any other failure to read
   the limits is `internal`.

## Contract

- No subject, header, stream or bucket name moves; META still records only
  an explicit override.
- Buckets (META, state, index) carry no byte budget, as before: a
  per-stream cap does not refuse them. An account with
  `max_bytes_required` would refuse them — and so cannot run chronicle.

## Tests

`TestLogBudget` (the arithmetic); `TestLogBudgetFollowsTheAccountsCap` on
a real server whose account carries `disk_max_stream_bytes` (default takes
the cap, an override inside it is kept, one above it is refused and the
name stays free); `TestLogBudgetWithoutACapIsTheDefault` (1 GiB without a
cap). `natstest.StartJetStreamLimited` runs the limited account.
