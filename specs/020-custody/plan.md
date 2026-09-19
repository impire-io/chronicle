# Plan 020 — custody is the AUTH bucket

## Layout

```
internal/mint/custody.go         Custody interface: Operator(), Account(name),
                                 Tenant(name), PutTenant(rec, expectedRev) …;
                                 Bucket implementation over KV_AUTH (Create /
                                 Update(rev) / Get / Watch); the templates
                                 ControlInstanceTemplate(), FleetTemplate()
                                 (jwt.UserPermissionLimits)
internal/mint/root.go            the offline root: init (generate, per-node JS
                                 keys, first bundle), seal (write, read back,
                                 shred), export; root.json manifest
internal/mint/bootstrap.go       Bootstrap becomes the in-memory birth `up`
                                 uses; LoadOrInitBootstrap retired
internal/mint/jwt.go             Driver: mint/revoke/rekey read from Custody,
                                 CAS-before-push, Reconcile(ctx)
internal/mint/rotate.go          rotate over the bucket; requireStopped removed;
                                 emits the new operator JWT beside the configs
internal/mint/clusterconfig.go   jetstream { cipher: chacha, key } per node
internal/mint/authbootstrap.go   CONTROL carries the external-authorization
                                 config; auth_users maintained at issuance;
                                 AUTH account removed
internal/control/*               reads Custody, never the dir; fleetcreds pulls
                                 tenant.<name>.service; bridge signs with
                                 CONTROL's key; reconcile loop
internal/fleet/controlplane.go   from PR #23, minus workloads.Start; bundle
                                 loading; --node-replicas
internal/fleet/fleet.go          Up on the same path: init in memory → embedded
                                 server (cipher on) → seal → bundle → control +
                                 workloads + executor
internal/fleet/*_test.go         two planes race revoke/rekey → no lost update;
                                 second plane from bundle+URL alone; fleet user
                                 fenced (operator mode); rotation live; boot
                                 reconcile; replicas_test.go (PR #24) green
cmd/chronicle/main.go            operator init | seal | instance add|remove |
                                 export | rotate-signing-key | emit-cluster-config
cmd/chronicle-control/main.go    thin: fleet.RunControl(--url --bundle …)
cmd/chronicle-workloads/main.go  new, thin over internal/workloads (0029)
internal/node/rollup.go          the losing replica declines cleanly
.goreleaser.yaml                 the sixth build row; comment names 0029
.golangci.yml                    cmd/chronicle-workloads rule; control rule
contrib/systemd/*                chronicle-workloads.service; control's flags
README.md, specs/018, specs/019  pointers follow
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

## Order — each step a green `make check`

1. `custody.go` + `root.go`: the store, the templates, init/seal/export
   against an embedded server; unit + integration tests. The dir still
   works for everything else.
2. The driver and control read the store; CAS-before-push; reconcile;
   `claimsMu` retired; the two-planes race test. `up` seals into its
   embedded JetStream and boots from a bundle it issued.
3. `operator instance add|remove`; the `fleet` template on workloads,
   executors, the CLI; the fence test in operator mode.
4. `cmd/chronicle-workloads`; `chronicle-control --url --bundle` without
   workloads; goreleaser six; contrib units; the late-executor boot test
   from spec 019 still green.
5. Rotation over the bucket, live; the rotation test.
6. The AUTH fold; spec 017's callout tests re-targeted.
7. `rollup.go` decline; `replicas_test.go` green; `--node-replicas`.
8. README and spec pointers; `make check`; mark ready. Close PR #23 and
   PR #24 as superseded with a pointer here.
