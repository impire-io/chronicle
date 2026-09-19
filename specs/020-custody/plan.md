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
