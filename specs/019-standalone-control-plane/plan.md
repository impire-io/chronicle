# Plan 019 — the standalone control plane

## Layout

```
internal/fleet/controlplane.go   ControlPlaneConfig{Dir, URL, GithubClientID,
                                 Logger}; ControlPlane{ctrl, wl, conns} + Stop;
                                 StartControlPlane(ctx, cfg) loads the bootstrap
                                 and composes over cfg.URL; startControlPlane(ctx,
                                 b, cfg, beforeControl) is the shared body —
                                 workloads first, beforeControl (up's embedded
                                 executor), then control with OnTenant dispatch +
                                 waitForNode and the bridge; PlacementTimeout;
                                 RunControl(ctx, args, out): the binary's flags
internal/fleet/fleet.go          Up composes server → executor backend →
                                 startControlPlane(…, startExecutor); Fleet
                                 holds srv, cp, ex, exConn; Stop in the same
                                 order as before; package doc names both roots
internal/fleet/controlplane_test.go
                                 TestStandaloneControlPlane: real server, plane
                                 with no executor, standalone in-process executor
                                 joins, mint → state; restart plane first,
                                 executor 1 s later → state and history again.
                                 TestStandaloneControlPlaneWritesBridgeProfile.
                                 TestRunControlNeedsAURL.
cmd/chronicle-control/main.go    thin: fleet.RunControl
.golangci.yml                    cmd-chronicle-control-runs-the-control-plane:
                                 reaches services only through internal/fleet
contrib/systemd/*.service|timer  nats-server, chronicle-control, chronicle-
                                 executor, chronicle-verify, chronicle-snapshot
contrib/verify-tenant.sh         the synthetic tenant's read (shellcheck clean)
contrib/snapshot-store.sh        cold/hot archive → rclone (shellcheck clean)
contrib/README.md                what runs where
specs/018-cluster-config/spec.md header: work id 09-hosted-environment, merged
README.md                        the multi-host section names --url and contrib/
```

## Order

1. `controlplane.go` + the `Up` refactor; `go build`, existing fleet tests
   green (the composition moved, behavior did not).
2. `controlplane_test.go` — the standalone ceremony and the boot race.
3. `cmd/chronicle-control` thin main; depguard rule; `make lint`.
4. `contrib/` units and scripts; shellcheck.
5. Spec headers, README; `make check`; draft PR → ready.
