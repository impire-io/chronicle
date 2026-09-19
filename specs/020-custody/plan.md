# Plan 020 — custody is the AUTH bucket

## Layout

```
internal/mint/custody.go         Custody over KV_AUTH: Operator(), Account(name)
                                 (seed, JWT, users by instance name), Auth(),
                                 Tenant(name); PutX(rec, expectedRev) …
internal/mint/templates.go       the four role templates (0032): control-
                                 instance, executor(name), workloads, cli —
                                 allow-lists; Template, ParseTemplate
internal/mint/instance.go        AddInstance(name, template) / RemoveInstance —
                                 users recorded on the account record by name;
                                 mutateAccount: CAS, re-sign and push on change
internal/mint/root.go            the dev dir: manifest, node key, bundles,
                                 export; Seal from material (the environment's
                                 seeds) — stamps the account shapes, issues the
                                 first bundle; no init/emit verbs
internal/mint/bootstrap.go       the in-memory generator `up` plays the
                                 environment with; mints no bootstrap users
internal/mint/jwt.go             Driver: mint/revoke/rekey read from Custody,
                                 CAS-before-push, Reconcile(ctx)
internal/mint/rotate.go          the service's rotation step over the live
                                 bucket; the dev shape composes the environment's
                                 steps around it; requireStopped gone
internal/mint/clusterconfig.go   the trio test's renderer only (cipher + key per
                                 node); not a CLI verb
internal/mint/authbootstrap.go   CONTROL carries the external-authorization
                                 config; auth_users maintained at issuance;
                                 AUTH account removed
contract/fleet.go                REGISTER/REPORT/CREDS subjects take the
                                 executor token; ExecutorFromSubject
internal/control/*               reads Custody, never the dir; serves CREDS.*
                                 and reads the caller from the subject; bridge
                                 signs with CONTROL's key; reconcile loop
internal/workloads/*             serves REGISTER.* / REPORT.* and reads the
                                 caller from the subject
internal/executor/*              publishes on its own subjects; its ID is its
                                 instance name
internal/fleet/controlplane.go   from PR #23, minus workloads.Start; bundle
                                 loading; --node-replicas
internal/fleet/fleet.go          Up on the same path: generate in memory →
                                 embedded server (cipher on) → seal → bundle →
                                 members as instances under their templates →
                                 control + workloads + executor
internal/fleet/operator.go       seal | instance add|remove | export |
                                 rotate-signing-key — the service's ceremonies
internal/fleet/*_test.go         two planes race revoke/rekey → no lost update;
                                 second plane from bundle+URL alone; the fence
                                 per role; seal from material on a trio;
                                 rotation live with both keys trusted; boot
                                 reconcile; replicas_test.go (PR #24) green
cmd/chronicle/main.go            operator seal | instance add|remove | export |
                                 rotate-signing-key
cmd/chronicle-control/main.go    thin: fleet.RunControl(--url --bundle …)
cmd/chronicle-workloads/main.go  new, thin over internal/workloads (0029)
internal/node/rollup.go          the losing replica declines cleanly
.goreleaser.yaml                 the sixth build row; comment names 0029
.golangci.yml                    cmd/chronicle-workloads rule; control rule
README.md, specs/018, specs/019  pointers follow; no contrib/ (chronicle-ops)
```

## Increment 1 — landed on this branch

The store (`custody.go`), the templates (`templates.go`), the root
(`root.go`: init, seal, export, bundles, node keys), encryption at rest in
every emitted config, and the three verbs — with tests on real servers:
eight-way compare-and-set with no lost update; the fence in operator mode
(open, direct get, publish, consumer all refused; the user's own bucket
works; a control instance reads); init → bundle connects → seal → read back
→ export → second seal matches → a foreign root is refused; the emitted
trio boots encrypted from the environment key. Three calls made while
building, recorded against the spec:

- **Node keys are born at emit, not at init.** `emit-cluster-config` is
  where nodes are named; the first emit of a node records its key in
  `root.json`. `init` stays a nodes-free birth.
- **The config carries no key.** It reads `$NATS_JETSTREAM_KEY` from the
  node's unit environment; the root records the value. A config file is
  world-readable by convention; a key in it would not be.
- **`seal --replicas` is required**, not defaulted: 3 on the trio, 1 on a
  single server, said by the operator reading the runbook — a silent R1
  custody bucket on a cluster is the failure a default would invite.
- **Seal does not yet shred the root's working seeds**: the driver and
  control still read the directory until increment 2 moves them; the
  shred lands with that move, so no intermediate state on this branch
  leaves a working install without its keys.

## Increment 2 — landed on this branch

The driver reads every key it signs with from the bucket and lands every
mutation there by compare-and-set before it pushes; control reads tenants
from the driver, replays custody at boot, reconciles the resolver at boot
and every five minutes, and holds no mutex; `up` walks design 10's first
boot in one process — root, encrypted embedded server, seal, the bundle's
users. The two-instance race (two drivers, one bucket, six rounds of
concurrent revocations) loses nothing; a stale resolver is put back by
Reconcile. Three calls made while building:

- **The driver issues the service user and records the tenant before it
  pushes**, so the record exists before the resolver knows the account and
  a taken name is the compare-and-set's refusal, not a directory check. A
  mint that cannot push takes its record back.
- **The directory rotation ceremony keeps a sealed root's bucket in step**
  (operator entry, every account's fresh JWT) during its verification boot,
  and now rewrites its own AUTH copy as it did SYS and CONTROL. This is a
  bridge to increment 5, where rotation runs live over the bucket and the
  directory ceremony goes.
- **The shred moves to increment 3** with the fleet users: the CLI, the
  workload service, and the executor still dial with the root's bootstrap
  control user until each has a `fleet`-template user of its own.

A warning seen in the fleet tests' output — a member connection exceeding
its subscription limit during the member lifecycle test — predates this
work (it prints on main) and is noted, not touched.

## Increment 3 — landed on this branch

The instance ceremony over the bucket — `operator instance add|remove
<name> [--template control-instance|fleet]` — the `fleet` template on
`up`'s own members and on every executor's and workload service's
credential, and the shred: seal leaves no working key in the root. On
real servers: a second control plane from a bundle and a URL alone mints,
and the first instance sees the tenant; a fleet instance reaches every
verb and never the bucket; removal evicts the live instance and its
bundle is refused; the name comes back with fresh keys; `up`'s workloads,
executor, and CLI users are fenced and reused across a restart; rotation
on a sealed root re-shreds. Calls made while building:

- **The account record lists its users by instance name.** `account.SYS`
  and `account.CONTROL` carry `users: {<instance>: <user public key>}`,
  written at seal for the bundles the root issued and by every `instance
  add` after. `remove` revokes by name: losing the host is how the bundle
  is lost, so the bundle cannot be what removal needs.
- **A fleet bundle is one file** — `control.creds` under the fleet
  template; no SYS user for a workload service, an executor, or the CLI.
  A bundle's template is read from its shape.
- **The ceremonies dial with the root's own control-instance bundle**,
  never through a control verb: a verb any fleet user could call would
  let a fleet user mint an instance. `remove` never runs as the instance
  it removes, and the root refuses to remove its last control instance.
- **`up`'s members are instances** — `workloads`, `executor-local`,
  `cli` — issued over the bucket on first boot and reused as bundles
  under the dev dir on every boot after. The CLI reads
  `bundles/cli/control.creds`.
- **Seal shreds** the working seeds and the bootstrap users' creds
  (zero-filled, removed); the root keeps `operator.nk`, `root.json`,
  `bundles/`, `exports/`, and public material. A seal on a shredded root
  verifies by public key — the operator signing key, every bootstrap
  account — and writes no export; the account JWTs are not compared,
  since the bucket's move on with every instance change and rotation.
  The bootstrap `control` and `sys` users are shredded, not revoked: the
  revocation lands with the fold (increment 6), where CONTROL's JWT is
  re-signed anyway.
- **`add` records only** before the fold: issuing a user changes nothing
  in the account JWT, so nothing is pushed. `remove` re-signs by
  compare-and-set and pushes.
- **The directory rotation ceremony** dials with the bundle and shreds
  the fresh seed once the bucket holds it — still the bridge to
  increment 5.

## Increment 4 — landed on this branch

The fence by role, as 0032 fixed it. Four allow-list templates in the mint
seam — `control-instance`, `executor(<id>)`, `workloads`, `cli` — and
`instance add --template <role>` issuing under them; `REGISTER`, `REPORT`
and `CREDS` carry the executor token, the workload service serves
`REGISTER.*` and `REPORT.*` and control serves `CREDS.*`, each reading the
caller from the subject; `init` mints no bootstrap pair, so a root's only
users are its bundles and the ceremonies dial with nothing else; `up`'s
members are instances under their roles. On real operator-mode servers:
each role's own work succeeds and its row's "never" is refused — an
executor registers, reports and pulls creds on its own subjects, and
cannot register as another, pull on another's subject, mint, dispatch, or
reach JetStream; a forged payload is refused with `caller-mismatch`; the
workload service runs the fleet log, serves dispatch and the bridge
report, and cannot mint, dispatch, or pull creds; the CLI mints and cannot
touch JetStream; every `up` member is fenced and named by its bundle. Calls
made while building:

- **The executor's ID is its credential's instance name.** `chronicle-
  executor` reads it from `--creds` and `--id` may only agree — the server
  lets the credential speak on that name's subjects and no other, so the
  ID is not a choice. `up`'s embedded executor is the instance `local`.
- **An absent payload name takes the subject's; a different one is
  refused.** The request types keep their `executor` field for the
  record's sake; the subject is what the fence binds.
- **The workloads role serves the bridge report too.** The node's index
  report arrives on `CHRONX.FLEET.REPORT.<tenant>` in the control account
  and the workload service answers it — one of "its own endpoints" that
  design 10's table names without spelling; the allow-list spells it.
- **The bucket half of the fence stays a mint test; the roles are a fleet
  test.** `TestRoleTemplatesFenceTheBucket` checks each template against
  the bucket on a bare server; `TestRoleTemplatesHold` runs control and the
  workload service under their credentials and drives every role's row.
- **The late-executor boot test from spec 019 is not on this branch** — it
  is PR #23's — and lands with the standalone plane in increment 5.

## Increment 5 — landed on this branch

The control plane stands alone and the workload service is its own
binary. `chronicle-control --url U --bundle DIR [--github-client-id]
[--bridge-profile]` is one control instance from its bundle and a URL:
control's verbs and its bridge over the bucket, the dispatch call that
keeps the mint's promise, and nothing else — `internal/fleet/
controlplane.go`, with `startControl` the one composition of control both
roots share and `up` composing the workload service and the executor
around it. `cmd/chronicle-workloads --url --creds` is the sixth binary,
thin over `internal/workloads`, with its goreleaser row, its archive and
brew entries, and its lint rule; `cmd/chronicle-control` is thin over
`fleet.RunControl` and reaches the services through `internal/fleet`
only. No `contrib/`: units are the environment's (0031). Tests: the
stand-up in miniature — a sealed substrate, then the workload service, a
control instance, and an executor host each joining with a credential of
its own role, the mint probe through a cli credential, and the hosted
boot order with the executor a second late converging through the level
scan (spec 019's test, carried); the bridge profile written where the
instance is told; the flag seam. Calls made while building:

- **`chronicle-control` refuses a bundle without `sys.creds`** by name —
  a one-file bundle is another role's — and names its instance from the
  credential in its banner and connection names.
- **The bridge profile is written where the instance is told**, `bridge.
  json` beside the bundle by default; `up` keeps writing it into the dev
  dir, where `chronicle login` looks.
- **The late-executor test restarts the workload service too**, since in
  the hosted form every unit restarts — the level scan re-auctions
  regardless of which came back first.

## Reshaped by 0031 and 0032 — before increment 4

The review of increment 3 asked whether a `fleet`-template credential was a
security issue. It was: a fleet user reached every control verb — a member
of any tenant, any placed tenant's service creds by naming the executor
that held them, the fleet log — and the bootstrap's own `control` and
`sys` users were shredded at seal but never revoked. Decision 0031 then
drew the line between the service and the environment, and 0032 fixed the
fence's shape. Increments 1–3 stand as control's own state. What follows
is the remaining order against design 10 @ `f56ad8c`; the old increments
4–8 are replaced, not renumbered — the contrib units are gone (the
environment's), the ceremonies split, and the fence comes first because
nothing else should land on the wrong side of it.

## Order — each step a green `make check`

1. ~~`custody.go` + `root.go`~~ — landed.
2. ~~The driver and control read the store~~ — landed.
3. ~~`instance add|remove`; the `fleet` template; the shred~~ — landed.
4. ~~**The fence by role** (chronicle-22)~~ — landed: the four templates as allow-lists,
   the `executor` one parameterized by name; `REGISTER`, `REPORT` and
   `CREDS` take the executor token — workloads serves `REGISTER.*` and
   `REPORT.*`, control serves `CREDS.*`, each reading the caller from the
   subject and refusing a mismatched payload; the executor publishes on
   its own subjects and its ID is its instance name; `instance add
   --template <role>`; `up`'s members under their roles; `init` mints no
   bootstrap pair and the test substrate (`natstest.StartOperator`) dials
   with the bundle. Tests: each role's own work succeeds and its row's
   "never" is refused, on a real operator-mode server; the late-executor
   boot race from spec 019 still green.
5. ~~**The standalone plane and the sixth binary**~~ — landed: `chronicle-control --url
   --bundle` without workloads; `cmd/chronicle-workloads --url --creds`;
   the goreleaser row; the lint rules. No `contrib/`.
6. **The ceremony split and the two-step rotation** (chronicle-21): `seal`
   takes `--signing-seed --sys-seed --control-seed`, stamps the shapes,
   pushes, issues `instance-1`, exports; `init` and `emit-cluster-config`
   leave the CLI, the renderer stays as the trio test's substrate, the
   node keys leave `root.json` (the trio test passes each node's key by
   environment, as the environment would); `rotate-signing-key --url
   --new-signing-seed` over the live bucket by compare-and-set and push;
   `requireStopped` and the directory ceremony go; the dev shape composes
   the environment's steps around the service's. Tests: seal from
   material on an embedded trio; rotation on a live fleet with both keys
   trusted and a mint mid-roll; every credential connects after.
7. **The AUTH fold**; spec 017's callout tests re-targeted.
8. **`rollup.go` decline**; `replicas_test.go` green; `--node-replicas`.
9. README and spec pointers; `make check`; mark ready. Close PR #23 and
   PR #24 as superseded with a pointer here.
