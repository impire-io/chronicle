# Spec 020 — custody is the AUTH bucket: control runs as n instances

**Work ID:** `10-custody` (chronicle-hq design 10)
**Design:** `chronicle-hq` @ `f56ad8c` —
[`02-DESIGN/10-custody.md`](../../../chronicle-hq/02-DESIGN/10-custody.md),
per decision
[`0030`](../../../chronicle-hq/03-DECISIONS/0030-custody-is-the-auth-bucket.md)
as amended by
[`0031`](../../../chronicle-hq/03-DECISIONS/0031-open-is-one-tenant-the-service-is-managed.md)
(the service owns what goes in the JWTs and the bucket; the environment
owns the cluster) and
[`0032`](../../../chronicle-hq/03-DECISIONS/0032-the-fence-is-per-role-and-a-fleet-request-names-its-caller.md)
(the fence is per role; a fleet request names its caller in its subject;
chronicle issues every platform user; rotation in two steps), under the
posture of
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
(the ceremony, split between the environment and the service). The research
that verified the load-bearing parts on real servers is chronicle-hq
`01-RESEARCH/010-shared-custody`.
**Status:** specified on this branch ([plan.md](plan.md)); increments 1–3
landed at design 10 @ `dd6b17c`; the remainder reshaped against `f56ad8c`
(0031, 0032); increment 4 — the fence by role — landed.
**Supersedes:** spec 019 / PR #23 (the standalone control plane, still
hosting the workload service) — its composition is carried here, reshaped;
its `contrib/` units are **not**: units are the environment's
(`chronicle-ops`, 0031). PR #24's two-replica test lands here, green.

## Constitution check

Against `AGENTS.md` and `how-we-build`: the wire a tenant sees does not
change (no subject, header, stream, or bucket a tenant touches moves); the
one new resource is the control account's `AUTH` bucket — ALL CAPS, the
stream `KV_AUTH`; three subjects of the fleet's own wire gain a caller
token (`CHRON.CTRL.FLEET.REGISTER|REPORT|CREDS.<executor>`), which no
tenant speaks; nothing enters the append path; every test that exercises
the bucket, the fence, the callout, or the rotation runs against a real
embedded server; the quality gate is unchanged. No conflict.

## What this delivers

Today `chronicle-control` cannot run twice: its custody is a directory on
one host. After this spec it runs as n peers, each holding one secret —
its credentials bundle — and everything else lives in the substrate; every
other member of the fleet holds a credential that reaches exactly its own
role's subjects; and nothing about running the NATS cluster is in the
product.

1. **The custody store.** `internal/mint` reads and writes the `AUTH`
   bucket in the CONTROL account (R3, history 16): `operator` (the
   operator signing seed), `account.SYS` and `account.CONTROL` (seed,
   current JWT, **and the users issued from it by instance name**), `auth`
   (the callout xkey), and `tenant.<name>` (public key, signing seed,
   scoped seed, service creds, **the canonical account JWT**). Every claims
   mutation — revoke, rekey, rotation, instance removal — is a KV
   `Update(revision)` on the entry **before** the push to
   `$SYS.REQ.CLAIMS.UPDATE`; a refused update re-reads and recomputes on
   top of the winner. Control reconciles bucket against resolver at boot
   and every few minutes. The in-process `claimsMu` is retired. The
   directory survives in one role: `up`'s in-memory birth and dev dir.
2. **The fence, per role (0032).** Four permission templates in the mint
   seam, allow-lists, stamped into every SYS- and CONTROL-account user
   chronicle issues — and chronicle issues every one; `init` mints no
   bootstrap pair:
   - `control-instance` — unrestricted.
   - `executor`, parameterized by the instance name `<id>`, which **is the
     executor's ID** — publish `CHRON.CTRL.FLEET.REGISTER.<id>`,
     `CHRON.CTRL.FLEET.REPORT.<id>`, `CHRON.CTRL.FLEET.CREDS.<id>`,
     `_INBOX.>`; subscribe `CHRON.CTRL.FLEET.AUCTION`,
     `CHRON.CTRL.FLEET.DELEGATE.<id>`, `CHRON.CTRL.FLEET.STATUS.<id>`,
     `CHRON.CTRL.FLEET.DESTROY.<id>`, `$SRV.>`, `_INBOX.>`. Nothing else:
     no JetStream API, no bucket, no verb.
   - `workloads` — publish `CHRON.>` (the fleet log's subjects, the
     auction, the executors' endpoints), `$JS.API.>`, `$KV.META.>`,
     `$KV.STATE_FLEET.>`, `_INBOX.>`; subscribe `CHRON.CTRL.FLEET.DISPATCH`,
     `CHRON.CTRL.FLEET.STOP`, `CHRON.CTRL.FLEET.REGISTER.*`,
     `CHRON.CTRL.FLEET.REPORT.*`, `$SRV.>`, `_INBOX.>`; **deny** publish and
     subscribe on `$KV.AUTH.>` and publish on every JetStream API subject
     naming `KV_AUTH` (the list increment 1 verified), and publish on
     `CHRON.CTRL.TENANT.>`, `CHRON.CTRL.MEMBER.>`, `CHRON.CTRL.FLEET.CREDS.>`.
   - `cli` — publish `CHRON.CTRL.TENANT.>`, `CHRON.CTRL.MEMBER.>`,
     `_INBOX.>`; subscribe `_INBOX.>`.
   **A fleet request that asserts a caller carries it in its subject:**
   `chronicle-workloads` serves `REGISTER.*` and `REPORT.*`, `chronicle-
   control` serves `CREDS.*`; each reads the executor from the subject's
   last token and refuses a payload naming another. The permission is the
   identity (tracker chronicle-22).
3. **The ceremonies**, dispatched from `cmd/chronicle` — the service's
   half of design 10 § first boot; the environment's half (`nsc`, the
   node configs, the node keys, the units, the operator JWT roll) is
   `chronicle-ops`'s and leaves this repo:
   - `chronicle operator seal --url U --replicas N --signing-seed F
     --sys-seed F --control-seed F [--out DIR]` — handed the environment's
     material: re-signs the two bare accounts with the service's shapes
     (JetStream limits, the bridge export, after the fold the callout
     config) and pushes them, creates `AUTH`, writes the working keys and
     the callout xkey it generates, reads every entry back, **issues the
     first control instance's bundle** to `--out` (default
     `./bundles/instance-1`, mode 0700), and writes a dated export.
     Idempotent: a second run compares by public key and reports.
   - `chronicle operator instance add|remove <name> --template
     control-instance|executor|workloads|cli [--url U] [--bundle DIR]` —
     run with a control instance's bundle against any node: issues (or
     revokes, by the name the account records list) the role's users,
     re-signs and pushes on removal, writes the bundle — a directory
     holding `control.creds` and, for a control instance, `sys.creds`.
   - `chronicle operator export --url U --out F` — the dated export.
   - `chronicle operator rotate-signing-key --url U --new-signing-seed F`
     — the service's step of the two-step rotation (5).
   - `chronicle operator init` and `emit-cluster-config` **leave the
     CLI**; `up` keeps an in-memory generator for its own birth, and the
     cluster-config renderer stays only as the trio test's substrate.
4. **The control plane stands alone, without the workload service.**
   `chronicle-control --url U --bundle DIR [--github-client-id ID]
   [--node-replicas N]`: control's verbs and the bridge over the bucket,
   the dispatch client call that keeps the mint's promise (spec 019's
   composition, verified for the late-executor boot race), and nothing
   else. **`chronicle-workloads` is its own binary** (`cmd/chronicle-
   workloads --url --creds`, thin over `internal/workloads`; the sixth
   goreleaser row, per 0029). `chronicle up` composes all of it in one
   process on the *same* path: keys and bare accounts in memory, the
   embedded server encrypted at rest under a node key kept in the dev
   dir, the service's seal, a bundle it issues itself, and its own members
   — `workloads`, `executor-local`, `cli` — as instances under their
   templates, issued once and reused across boots. No `contrib/` in this
   repo: units are the environment's.
5. **Rotation, clustered, in two steps.** The environment adds the new
   signing key to the operator JWT and rolls the nodes; `chronicle
   operator rotate-signing-key --url U --new-signing-seed F` lands the seed
   in `operator` by compare-and-set and re-signs every account JWT — SYS,
   CONTROL, every tenant — each by compare-and-set then push; the
   environment removes the old key and rolls again. `requireStopped` and
   the stopped-directory ceremony go; mints during the roll sign under the
   key the bucket names, and both keys are trusted throughout (tracker
   chronicle-21). In the dev shape the same verb, given `--dir`, plays the
   environment's two steps around the service's over the embedded server.
6. **The AUTH account folds into CONTROL.** CONTROL's JWT carries the
   external-authorization config (issuer, `allowed_accounts: ["*"]`, the
   xkey); the bridge signs placed users with CONTROL's account key; every
   user chronicle issues is listed in `auth_users` at issuance; the
   sentinel is the one CONTROL user that is not, and is therefore always
   gated. `auth-account.*`, `bridge.creds` and the AUTH preload go; spec
   017's callout tests re-target CONTROL.
7. **A rollup that loses to a peer declines cleanly.** `internal/node/
   rollup.go` treats a replay `Next` timeout — or the subject's first
   sequence moving past its cursor — as *lost the race*, the same clean
   decline the guard path returns. The two-replica test from PR #24 lands
   here and passes; `--node-replicas` is a control flag whose value the
   environment sets.

## Contract

- **The tenant wire is unchanged.** Member creds, the bridge's placed
  users, `Op-Author`, every `CHRON.>` subject a tenant speaks: issued and
  trusted exactly as before. What moves is where the issuer keeps its keys
  and three subjects of the fleet's own wire.
- **The bucket is canonical; the resolver is a projection.** No code path
  pushes an account JWT it did not first land in the bucket by compare-
  and-set. Two instances mutating one tenant concurrently lose nothing —
  tested with two control planes over one embedded server racing revokes
  and rekeys.
- **Every platform user carries its role's template, and no other user
  exists** — tested per role in operator mode: an executor's credential
  registers, reports, bids, serves its endpoints, and pulls its own creds,
  and cannot mint, add a member, pull another executor's creds, read or
  write the fleet log, or open the bucket; a workloads credential runs the
  fleet log and cannot mint or pull creds; a cli credential mints and
  cannot touch JetStream; after seal the two platform accounts list
  exactly the users the bucket records.
- **A control host's only secret is its bundle** — tested by starting a
  second control plane from a bundle and a URL alone, against a bucket
  the first sealed, and minting through it.
- **Seal from the environment's material** — tested on an embedded trio
  booted from configs the test renders: bare accounts and seeds in, the
  shapes stamped and served, the first bundle connecting, the bucket at
  R3.
- **Rotation on a live fleet** — tested: both keys trusted, the service's
  step against a running server, every credential the install holds
  connects after, a mint during the roll succeeds.
- **The callout serves from CONTROL** — spec 017's device-flow and
  placement tests pass with no AUTH account in the bootstrap.
- **Every test runs against real embedded NATS**; mocking the NATS
  client stays forbidden.

## Out of scope

- The environment: rendering node configs, node keys, TLS, units,
  snapshots, the operator identity and the operator JWT roll — `chronicle-
  ops` (0031); the hosted runbook (chronicle-hq PR #15) is reshaped there.
- Client-side sealing of bucket values, an HSM, customer-held keys — 0030
  keeps them as later records.
- `tenant destroy` (chronicle-20) — it will also remove `tenant.<name>`;
  its own spec.
- Rotating a node's at-rest key in place — by rebuild, per the design.
- The repo split (chronicle-23): this branch lands in `chronicle` as it is
  today; the split moves what it built.
- Invites, the panel, the SDKs.
