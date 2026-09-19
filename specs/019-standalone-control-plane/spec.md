# Spec 019 — the standalone control plane, and the units it runs as

**Work ID:** `09-hosted-environment` (chronicle-hq design 09)
**Design:** `chronicle-hq` @ `ab5868c` —
[`02-DESIGN/09-hosted-environment.md`](../../../chronicle-hq/02-DESIGN/09-hosted-environment.md)
§ the stand-up ceremony, § the operational floor, per decision
[`0027`](../../../chronicle-hq/03-DECISIONS/0027-the-hosted-environment-runs-on-our-own-cluster-at-scaleway.md)
("whatever the stand-up runbook shows is missing"); the composition it
externalizes is [`06-scheduler.md`](../../../chronicle-hq/02-DESIGN/06-scheduler.md)
§ both forms.
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

The gap the stand-up runbook surfaced first: `chronicle-control` on its
own could mint an account but never place its node. Everything the
design's workload host runs beside control — the workload service, the
tenant-node dispatch that keeps the mint's promise, the browser identity
bridge — existed only inside `chronicle up`, and `up` always embeds its
own server. The hosted environment's mint probe could not pass.

1. **One control plane, both roots.** `internal/fleet` gains the
   composition `up` already ran after its server was up — the workload
   service, control with its `OnTenant` dispatch and verify-by-connect
   wait, the bridge and its profile hand-out — as a `ControlPlane` that
   `up` composes over its embedded server and `chronicle-control`
   composes over the operator's cluster. Same wiring, same placement
   timeout, no scheduler-shaped special case in either.
2. **`chronicle-control --dir D --url U [--github-client-id ID]`** is that
   plane standing alone. `--url` is the client url tenants connect to —
   control stamps it into META provisioning and dials it for the
   verify-by-connect read, so in the hosted form it is the public name.
   Absent, the dir's recorded url serves, exactly as before. The binary's
   main is now thin over `internal/fleet`, the way `cmd/chronicle` is; the
   depguard rule moves with it.
3. **The hosted boot order is absorbed, not assumed.** Control's boot
   replay dispatches the tenants on disk; a host's executor is its own
   unit and may register a beat later. The auction finds no bidder, the
   level scan re-auctions once it has one, and the replay's wait
   (`PlacementTimeout`, 45 s) covers it — a control unit that came up
   first does not fail. Tested against real servers.
4. **`contrib/`: the units and the floor's two scripts**, generic and
   address-free. `nats-server.service` (with `reload` for renewed
   certificates), `chronicle-control.service`, `chronicle-executor.service`
   (kvm group, libkrunfw path, guest binary path), the verification read
   `verify-tenant.sh` with its 15-minute timer — append, fold-to-state,
   index query, through a saved context as a tenant would — and
   `snapshot-store.sh` with its nightly timer: a cold archive around a
   stopped unit on NATS nodes, hot and age-encrypted for the custody dir,
   checksummed, uploaded with rclone, pruned.

## Contract

- **The control plane's wire surface does not change.** Every endpoint,
  subject, and account `up` served is served identically standalone; the
  only difference is whose server it stands over.
- **`chronicle-workloads` is served from the control unit.** Decision
  0017's five binaries stay five; the workload service is a micro service
  discoverable by name whatever process hosts it, and one instance is
  the present need. A second instance — or its own binary — is a later
  increment, not a quiet edit here.
- **The mint's promise holds standalone**: a minted tenant answers verbs
  before the mint returns, or the mint fails. Verified by a test that
  stands a control plane over a real server with no executor anywhere,
  joins a standalone executor over the wire, mints, reads state — then
  restarts control first and the executor a second later, and reads
  again. Could not succeed if the composition or the boot race were
  wrong.
- **Custody is unchanged.** The bootstrap dir stays the one place seeds
  live; the units run it mode 0700 under a dedicated user; the snapshot
  script refuses to ship it unencrypted only by convention (the
  recipient variable), which the runbook makes mandatory.

## Out of scope

- A scoped executor user. The executor still dials with the install's
  control-account user, as `up` does; the narrower credential design 06
  describes is a custody increment on its own.
- The clustered signing-key rotation ceremony (0024 clustered) — stand-up
  gated, before the first paying tenant, its own spec.
- Host provisioning, DNS, certificate issuance, and the environment's
  inventory — the runbook in chronicle-hq, never this repo (0027: the
  public repo carries no environment state).
- Shipping `contrib/` inside the release archive — the archive matrix is
  0017's; the runbook fetches these files from the tag.
- The websocket listener (design 09 names its moment: the panel's).
