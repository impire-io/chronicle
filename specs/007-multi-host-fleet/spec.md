# Spec 007 — the multi-host fleet: the bridge and the standalone executor

**Work ID:** `chronicle-3` (tracker item chronicle-3)
**Design:** `chronicle-hq` @ `1738709` —
[`02-DESIGN/06-scheduler.md`](../../../chronicle-hq/02-DESIGN/06-scheduler.md)
(§ the dispatch surface: the tenant-stamped service import; § the
executors: operator-started infrastructure, one per host) and
[`03-DECISIONS/0014-the-fleet-runs-on-chronicle.md`](../../../chronicle-hq/03-DECISIONS/0014-the-fleet-runs-on-chronicle.md)
(part 2: the bridge outside `CHRON.>`; part 3: per-host executors).
**Status:** in progress on this branch ([plan.md](plan.md)).

## What this delivers

The two pieces that make 0014's premise — heterogeneous executors on
many hosts — assemblable, and the death of spec 006's named stand-in.

- **The tenant-stamped bridge**, exactly as designed: a new reserved
  root **`CHRONX.>`** for cross-account bridge subjects — deliberately
  outside `CHRON.>`, because the member baseline grants that root to
  every member and reporting belongs to service users alone (members
  cannot publish `CHRONX.>` by construction; service users are
  unrestricted). The control account **exports** the service
  `CHRONX.FLEET.REPORT.*`; every tenant account JWT minted from now on
  **imports** it with the local subject `CHRONX.FLEET.REPORT` mapped to
  `CHRONX.FLEET.REPORT.<tenant>` — the stamp is server-side and
  unforgeable because chronicle signs the tenant JWT (0006): tenant A
  physically cannot report as tenant B. One export in the control
  account, one import per tenant JWT — O(1) per tenant. The export is
  unguarded on this substrate because only chronicle's signing key can
  mint an importer; a token-gated export is the `synadia` driver's
  concern when it exists.
- **The node reports over the bridge by default**: `node.Config` builds
  its reporter from the node's own connection when none is injected —
  the same handler-writes-META-and-reports flow, now on its sanctioned
  transport, identical in-process and in a microVM. Report failures are
  soft everywhere (a warning, never a refused declaration and never a
  failed boot): declarations are the truth, the level heals — the boot
  re-derivation now runs unconditionally and tolerates an absent bridge,
  so a node on a substrate without the fleet machinery still serves.
- **`chronicle-workloads` serves the bridge**: an endpoint on the
  stamped wildcard translates a report — `{action, log, index, kind}` —
  into the dispatch or stop the record needs, with the tenant taken from
  the subject token, never the payload. Unknown kinds are warned and
  left alone (read-side tolerance); the report is the request, the op is
  the record.
- **`internal/fleet/metareporter.go` and the composition's
  `indexReporter` are deleted** — one report path for every node,
  wherever it runs.
- **`cmd/chronicle-executor`**: the standalone form of the fleet's
  muscle — `--url --creds <control-account user> --id --backend
  inprocess|microsandbox [--workload-binary --image]`. An operator
  starts one per host (systemd, cloud-init, a terminal); it registers,
  bids, and carries placements exactly as the embedded executor does,
  because it *is* the embedded executor with its own main. The
  control-account creds are handed by the operator — per-executor users
  await the account-seed custody work the multi-host hardening owns.

## Wire tests

Against a real operator-mode server (the bridge is account machinery —
nothing less proves it): a minted tenant's service user reports over
`CHRONX.FLEET.REPORT` and the workload service receives it stamped
`.<tenant>` and dispatches (the record shows the workload); a member
baseline user **cannot** publish the bridge subject (no responder — the
permission boundary holds); a forged tenant stamp is impossible to
express (the import maps the local subject, the tenant never addresses
the stamped form). The composition tests keep passing on the one report
path.

## Out of scope

Per-executor control-account users (needs account-seed custody in the
bootstrap — a deliberate follow-up); executor tags and placement
constraints (no consumer pins workloads yet); detached msb sandboxes
surviving executor restarts; the `synadia` driver's import shaping
(token-gated export) — carried as 0014's build-time verify item until
that driver exists; re-minting pre-bridge tenants (dev dirs are
throwaway; an account-refresh flow is release-era work); docker and
kubernetes backends (triggers unfired).
