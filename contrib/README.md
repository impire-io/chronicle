# contrib — running the fleet as units

Install material for a composed install: the systemd units the fleet
binaries run as, and the two scripts the operational floor of
[`chronicle-hq/02-DESIGN/09-hosted-environment.md`](../../chronicle-hq/02-DESIGN/09-hosted-environment.md)
names. Everything here is generic — no addresses, no environment state —
and composes from the released binaries every operator gets (decision
[0017](../../chronicle-hq/03-DECISIONS/0017-release-flow.md)). The hosted
environment's own inventory and runbook live in chronicle-hq, not here.

| File | Runs on | What it is |
|---|---|---|
| `systemd/nats-server.service` | every NATS node | `nats-server` from the node's emitted config; `reload` re-reads TLS certificates |
| `systemd/chronicle-control.service` | the control host | the control plane: control, bridge, one `chronicle-workloads` |
| `systemd/chronicle-executor.service` | every workload host | the host's executor on its one backend |
| `systemd/chronicle-verify.{service,timer}` + `verify-tenant.sh` | the control host | the synthetic tenant's verification read, every 15 minutes and after every change |
| `systemd/chronicle-snapshot.{service,timer}` + `snapshot-store.sh` | every host with state | nightly cold snapshot to object storage; encrypted where seeds live |

Install the scripts as `/usr/local/bin/chronicle-verify-tenant` and
`/usr/local/bin/chronicle-snapshot-store`; each unit's header names the
`/etc/chronicle/*.env` file it reads.
