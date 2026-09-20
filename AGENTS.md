# Agent guide for chronicle

Durable instructions for any coding agent working in this repository. The full
rules live in `../chronicle-hq/00-META/`; this file is the orientation and the
non-negotiables.

## Orientation (read in this order)

1. [`../chronicle-hq/00-META/mission.md`](../chronicle-hq/00-META/mission.md) —
   what chronicle is and refuses to become.
2. [`../chronicle-hq/00-META/how-we-build.md`](../chronicle-hq/00-META/how-we-build.md) —
   the binding engineering postures: minimal build, ops-log as source of truth,
   every component a NATS micro service, headless. When this repo and that
   document disagree, that document wins.
3. [`../chronicle-hq/00-META/repos.md`](../chronicle-hq/00-META/repos.md) — the
   repo map and what this repo owns: the **open tenant plane** and nothing of
   the managed service ([`02-DESIGN/11-the-two-forms.md`](../chronicle-hq/02-DESIGN/11-the-two-forms.md),
   decision [0031](../chronicle-hq/03-DECISIONS/0031-open-is-one-tenant-the-service-is-managed.md)).
4. [`../chronicle-hq/02-DESIGN/`](../chronicle-hq/02-DESIGN/) — the designs
   this code implements, numbered in reading order. Capabilities land here
   through the build handoff
   ([playbook 04](../chronicle-hq/00-META/process/04-build-handoff.md)), not by
   invention in this repo.

## Non-negotiables

- **The boundary is the tenant.** Nothing here creates tenants, issues
  credentials, places workloads across hosts, or knows the managed
  service's repositories exist. The packages are public because the
  service composes them; the dependency runs one way, and `.golangci.yml`
  states the seams between them.
- **Quality gate before "done"**: `make fmt && make test && make lint` — all
  green, no skipped tests, race detector on.
- **The wire contract is tested against real NATS** (embedded server in
  tests); mocking the NATS client is forbidden.
- **The wire contract is near-immutable** (decision
  [0008](../chronicle-hq/03-DECISIONS/0008-tenant-data-plane-contract.md)):
  subjects, headers, stream and bucket names, and META key grammar come from
  the designs, never from convenience. Resource names — streams, KV buckets,
  object-store buckets — are ALL CAPS with underscores (`LOG_MY_LOG`, `META`,
  `STATE_MY_LOG`); subject identifiers stay lowercase on the wire.
- **Any NATS, any auth mode.** The client and the binaries take a creds
  file or an nkey with the principal stated; a change that assumes
  operator mode, a minter, or a fleet is on the wrong side of the line.
- **Minimal build**: every addition names the present need it serves; "we
  might want it later" is not a need.
- **Nothing sits in the append path**: appends are direct JetStream
  publishes; dedup and CAS guards are the server's, never proxied or
  re-implemented.
- **Commits are signed and signed off** (`git commit -S -s` — the DCO,
  [CONTRIBUTING.md](CONTRIBUTING.md)). `.claude/settings.local.json` is
  never committed.
- **Work follows [playbook 07](../chronicle-hq/00-META/process/07-parallel-work.md)**:
  one work ID as branch, workspace, and PR label; draft PR from the first
  push; a human merges.
