# Spec 020 — custody is the AUTH bucket: control runs as n instances

**Work ID:** `10-custody` (chronicle-hq design 10)
**Design:** `chronicle-hq` @ `dd6b17c` —
[`02-DESIGN/10-custody.md`](../../../chronicle-hq/02-DESIGN/10-custody.md),
per decision
[`0030`](../../../chronicle-hq/03-DECISIONS/0030-custody-is-the-auth-bucket.md)
under the posture of
[`0028`](../../../chronicle-hq/03-DECISIONS/0028-every-service-runs-as-n-instances.md)
(every service runs as n instances; nothing holds single custody) and
[`0029`](../../../chronicle-hq/03-DECISIONS/0029-the-workload-service-is-its-own-binary.md)
(the workload service is its own binary); the amended designs are
[`01-onboarding.md`](../../../chronicle-hq/02-DESIGN/01-onboarding.md) § custody
and § rotation, [`04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md)
(control's row, the node's rollups),
[`08-browser-identity.md`](../../../chronicle-hq/02-DESIGN/08-browser-identity.md)
(the AUTH account folds), and
[`09-hosted-environment.md`](../../../chronicle-hq/02-DESIGN/09-hosted-environment.md)
(the ceremony). The research that verified the load-bearing parts on real
servers is chronicle-hq `01-RESEARCH/010-shared-custody`.
**Status:** specified on this branch ([plan.md](plan.md)); implementation
follows in the plan's order.
**Supersedes:** spec 019 / PR #23 (the standalone control plane, still
hosting the workload service) — its composition and `contrib/` units are
carried here, reshaped; and PR #24's two-replica test lands here, green.

## Constitution check

Against `AGENTS.md` and `how-we-build`: the wire a tenant sees does not
change (no subject, header, stream, or bucket a tenant touches moves); the
one new resource is the control account's `AUTH` bucket — ALL CAPS, the
stream `KV_AUTH`; nothing enters the append path; every test that
exercises the bucket, the fence, the callout, or the rotation runs against
a real embedded server; the quality gate is unchanged. No conflict.

## What this delivers

Today `chronicle-control` cannot run twice: its custody is a directory on
one host. After this spec it runs as n peers, each holding one secret —
its credentials bundle — and everything else lives in the substrate.

1. **The custody store.** `internal/mint` reads and writes the `AUTH`
   bucket in the CONTROL account (R3, history 16): `operator` (the
   operator signing seed), `account.SYS` and `account.CONTROL` (seed +
   current JWT), `auth` (the callout xkey), and `tenant.<name>` (public
   key, signing seed, scoped seed, service creds, **the canonical account
   JWT**). Every claims mutation — revoke, rekey, rotation — is a KV
   `Update(revision)` on the tenant's entry **before** the push to
   `$SYS.REQ.CLAIMS.UPDATE`; a refused update re-reads and recomputes on
   top of the winner. Control reconciles bucket against resolver at boot
   and every few minutes (push when the bucket's JWT is newer). The
   in-process `claimsMu` is retired. The directory survives in two roles
   only: the **offline root** (operator identity, per-node JetStream keys,
   a dated export) and `up`'s in-memory birth.
2. **The fence.** Two permission templates in the mint seam, stamped into
   every CONTROL-account user chronicle issues: `control-instance`
   (unrestricted) and `fleet` — publish and subscribe denied on
   `$KV.AUTH.>`, publish denied on `$JS.API.STREAM.*.KV_AUTH`,
   `$JS.API.STREAM.*.KV_AUTH.>`, `$JS.API.CONSUMER.*.KV_AUTH.>`,
   `$JS.API.DIRECT.GET.KV_AUTH.>`. Workload-service instances, executors,
   and the operator's CLI carry `fleet` (tracker chronicle-22).
3. **The ceremonies**, dispatched from `cmd/chronicle` like the operator
   verbs that exist:
   - `chronicle operator init --root R --node <name>=<host> …` — today's
     `LoadOrInitBootstrap` split into *generate* and *do not keep*: the
     operator identity and signing key, the SYS and CONTROL seeds, the
     callout xkey, the first instance's bundle, one JetStream key per
     node; then the emitted node configs, which now carry
     `jetstream { cipher: chacha, key }`. The existing
     `emit-cluster-config` stays the rendering step and reads the root.
   - `chronicle operator seal --root R --url U` — creates `AUTH`, writes
     the working keys, reads every entry back, **shreds the working seeds
     from the root**, and writes the first dated export. Idempotent: a
     second run compares and reports.
   - `chronicle operator instance add|remove <name> [--template
     control-instance|fleet]` — on any live instance: issues (or revokes)
     a CONTROL user and a SYS user, re-signs and pushes the account JWTs,
     writes the bundle — a directory holding `control.creds` and
     `sys.creds`, mode 0700.
   - `chronicle operator export --url U --out F` — the dated export of
     the bucket for the offline root.
4. **The control plane stands alone, without the workload service.**
   `chronicle-control --url U --bundle DIR [--github-client-id ID]
   [--node-replicas N]`: control's verbs and the bridge over the bucket,
   the dispatch client call that keeps the mint's promise (spec 019's
   composition, verified for the late-executor boot race), and nothing
   else. **`chronicle-workloads` is its own binary** (`cmd/chronicle-
   workloads --url --creds`, thin over `internal/workloads`; the sixth
   goreleaser row, per 0029). `chronicle up` composes all of it in one
   process on the *same* path: init in memory, embedded server (encrypted
   at rest with a key kept in the dev dir), seal into the embedded
   JetStream, a bundle it issues itself; the dev dir keeps what the offline
   root keeps and nothing else. `contrib/` (from PR #23) gains the
   workloads unit and follows the new flags.
5. **Rotation, clustered.** `chronicle operator rotate-signing-key --root
   R --url U` runs against the live bucket: the new signing key to
   `operator`, every account JWT re-signed and landed by compare-and-set
   then pushed, the re-issued operator JWT written beside the node
   configs, and the rolling restart named in its output. `requireStopped`
   goes; mints during the roll sign under the key the bucket names
   (tracker chronicle-21).
6. **The AUTH account folds into CONTROL.** CONTROL's JWT carries the
   external-authorization config (issuer, `allowed_accounts: ["*"]`, the
   xkey); the bridge signs placed users with CONTROL's account key; every
   fleet user chronicle issues is listed in `auth_users` at issuance; the
   sentinel is the one CONTROL user that is not, and is therefore always
   gated. `auth-account.*`, `bridge.creds` and the AUTH preload go; spec
   017's callout tests re-target CONTROL.
7. **A rollup that loses to a peer declines cleanly.** `internal/node/
   rollup.go` treats a replay `Next` timeout — or the subject's first
   sequence moving past its cursor — as *lost the race*, the same clean
   decline the guard path returns. The two-replica test from PR #24 lands
   here and passes.

## Contract

- **The tenant wire is unchanged.** Member creds, the bridge's placed
  users, `Op-Author`, every `CHRON.>` subject: issued and trusted exactly
  as before. What moves is where the issuer keeps its keys.
- **The bucket is canonical; the resolver is a projection.** No code path
  pushes an account JWT it did not first land in the bucket by compare-
  and-set. Two instances mutating one tenant concurrently lose nothing —
  tested with two control planes over one embedded server racing revokes
  and rekeys.
- **`fleet` users cannot reach the bucket** — tested in operator mode
  with a `fleet`-template user, positively (its own JetStream work
  succeeds) and negatively (read and write refused).
- **A control host's only secret is its bundle** — tested by starting a
  second control plane from a bundle and a URL alone, against a bucket
  the first sealed, and minting through it.
- **Rotation on a live fleet** — tested: rotate against a running
  embedded server, every credential the install holds connects after,
  a mint during the roll succeeds.
- **The callout serves from CONTROL** — spec 017's device-flow and
  placement tests pass with no AUTH account in the bootstrap.
- **Every test runs against real embedded NATS**; mocking the NATS
  client stays forbidden.

## Out of scope

- The hosted runbook and inventory (chronicle-hq PR #15, reshaped after
  this lands); host provisioning; DNS; certificates.
- Client-side sealing of bucket values, an HSM, customer-held keys — 0030
  keeps them as later records.
- `tenant destroy` (chronicle-20) — it will also remove `tenant.<name>`;
  its own spec.
- Rotating a node's at-rest key in place — by rebuild, per the design.
- Invites, the panel, the SDKs.
