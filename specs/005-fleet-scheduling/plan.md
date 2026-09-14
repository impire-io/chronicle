# Plan 005 — fleet scheduling

The implementation order, each step green before the next. The wire truth
lands first; every service reads it from `contract`.

1. **Contract** (`contract/fleet.go`): the fleet log's name and subjects
   (`FleetLog`, executor/workload thing tails), the merge op types
   (`workload.assign/release/stop`), the state shapes (`WorkloadState`
   with kind/tenant/log/index/replicas/slots/failures, `ExecutorState`),
   the control-plane subjects (`CHRON.API.FLEET.*`), and the
   dispatch/bid/delegate/report/creds payload types. Tests: grammar and
   round-trips beside the existing contract tests.
2. **Fleet-log bootstrap** (control): provision on start — control META
   (`log.fleet.config`, the three merge type schemas with
   `effect: merge`), `LOG_FLEET` with the wire contract's stream
   settings, `STATE_FLEET`. Idempotent (create-or-open).
3. **`internal/workloads`**: the service. Fold (ordered consumer from 1,
   shared judge, in-memory state + `STATE_FLEET` CAS writes); the writer
   (single append path stamping the guard from the fold's horizon);
   DISPATCH/STOP endpoints; the auction (scatter, window, rank by bid
   score, delegate, accept, assign); REPORT endpoint (release with
   reason, failure counters into merged state); the level scan
   (unfilled → auction; assigned → liveness + backend-status witnesses →
   release(liveness) + re-auction; unschedulable → slow retry).
4. **`internal/executor`**: micro service (ping = liveness witness), the
   auction subscription + bid (live placement count as the score),
   DELEGATE/STATUS/DESTROY endpoints, the actuator seam
   (`ensure/status/destroy`), local restart with backoff and budget →
   REPORT on exhaustion, and the **backend zero** actuator: creds pull →
   `mint.ConnectCreds` → `node.Start` / `search.Start` per kind.
5. **Control**: the CREDS endpoint (STATE_FLEET read, tail-fold
   fallback, then the tenant's `service.creds`); mint path dispatches the
   `node` workload instead of starting one (`OnTenant` becomes the
   dispatch call, boot replay included).
6. **Node reports**: `node.Config` gains the reporter; INDEX
   DECLARE/DELETE call it beside the META write; boot re-derives the
   slice from `index.>`. `internal/fleet/indexes.go` is deleted.
7. **Composition** (`internal/fleet`): `Up` = server + control +
   workloads + executor; the reporter wiring; `Stop` order (executor
   placements last so verbs drain first is *not* required — everything is
   crash-only; stop is workloads, executor, control, server).
8. **Wire tests** (embedded NATS, race): the full slice
   (mint → node runs → declare → indexer serves → delete → retires);
   guard rejection (two assigns, one loses); restart recovery (executor
   comes back empty → release + re-auction); creds verification
   (unassigned executor refused); report → failing surfaces in
   `STATE_FLEET`. Existing suites stay green untouched.

Gate: `make check` (fmt, tidy, build, test -race, lint) before every
commit; live read at the end — `chronicle up`, mint, thing, declare,
query — before the PR flips ready.
