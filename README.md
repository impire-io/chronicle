# chronicle

The chronicle product code: the service fleet that runs ops-logs and their
declared indexes, the workload scheduler with its pluggable backends, the
client surface, the CLI, and the web control panel.

## Quick start (walking skeleton)

```
make build                       # binaries land in bin/
bin/chronicle up                 # bootstrap NATS + control + nodes, one process
# in another terminal:
bin/chronicle tenant create acme                       # writes acme-admin.creds
bin/chronicle log create orders --creds acme-admin.creds
bin/chronicle thing create orders invoice-1 --creds acme-admin.creds --state '{"total":3}'
bin/chronicle append orders invoice-1 comment.add --creds acme-admin.creds --payload '{"body":"hi"}'
bin/chronicle state orders invoice-1 --creds acme-admin.creds
bin/chronicle replay orders invoice-1 --creds acme-admin.creds
```

This repo exists by decision
[0010](../chronicle-hq/03-DECISIONS/0010-chronicle-repo.md) of
[`chronicle-hq`](https://github.com/impire-io/chronicle-hq) — the source of
truth for mission, research, designs, and decisions. Capabilities land here
through the build handoff
([playbook 04](../chronicle-hq/00-META/process/04-build-handoff.md)), not by
invention in this repo.

Agents start at [AGENTS.md](AGENTS.md).
