# chronicle

Chronicle is ops-logs as a product: every tenant gets append-only event
logs whose folded state, declared indexes (search, graph, semantic), and
scheduled workloads run as a fleet of NATS micro services. There is no web
UI yet and no side door: a CLI over the one client surface, tenants as
NATS accounts, and a local fleet that bootstraps its own NATS server — no
NATS expertise required to start.

This repo exists by decision
[0010](../chronicle-hq/03-DECISIONS/0010-chronicle-repo.md) of
[`chronicle-hq`](https://github.com/impire-io/chronicle-hq) — the source of
truth for mission, research, designs, and decisions. Capabilities land here
through the build handoff
([playbook 04](../chronicle-hq/00-META/process/04-build-handoff.md)), not by
invention in this repo. Agents start at [AGENTS.md](AGENTS.md).

## Getting started

**1. Get the binaries.** One brew install gets the whole fleet —
`chronicle` (the CLI, which also runs the local fleet) and the standalone
service binaries:

```sh
brew install impire-io/tap/chronicle
```

Or download the archive for your platform from the
[releases page](https://github.com/impire-io/chronicle/releases), or build
from source:

```sh
go install github.com/impire-io/chronicle/cmd/chronicle@latest
```

(`go install` builds report version `0.0.0-dev`; the released binaries
carry the tag's version — `chronicle version` says which you have.)

**2. Run the fleet.**

```sh
chronicle up
```

That single process is the whole thing: it bootstraps its own
operator-mode NATS server with JetStream, the control plane, a node, and
an embedded executor. Nothing to install or configure first — keys, creds,
and data live under `~/.chronicle/dev` (change with `--dir`), the server
listens on 4222 (`--port`, `-1` picks a free one), and it runs in the
foreground until interrupted.

**3. Mint a tenant and work with it** — in another terminal:

```sh
chronicle tenant create acme          # mints the account, writes acme-admin.creds
```

The creds file is the tenant: every client verb authenticates with
`--creds` and finds the fleet through the data dir (or an explicit
`--url`). Then:

```sh
chronicle log create orders --creds acme-admin.creds
chronicle thing create orders invoice-1 --creds acme-admin.creds --state '{"total":3}'
chronicle append orders invoice-1 comment.add --creds acme-admin.creds --payload '{"body":"hi"}'
chronicle state orders invoice-1 --creds acme-admin.creds
chronicle replay orders invoice-1 --creds acme-admin.creds
```

A *log* is an append-only event stream; a *thing* is one entity in it.
`append` writes an op, the node folds ops into state, `state` reads the
fold, `replay` walks the history. Op types can carry a schema and a
declared effect — appends failing the schema are refused before the wire,
and fully merge-covered things compact:

```sh
chronicle schema set orders comment.add --creds acme-admin.creds \
  --schema '{"type":"object","required":["body"]}'
chronicle schema set orders status.set --creds acme-admin.creds \
  --schema '{"type":"object"}' --effect merge
chronicle thing rollup orders invoice-1 --creds acme-admin.creds
```

**4. Declare indexes.** An index is declared on a log and built by the
fleet; search is the default kind:

```sh
chronicle index declare orders text --creds acme-admin.creds
chronicle index query orders text widgets --creds acme-admin.creds
```

The graph and semantic kinds are declared the same way with `--kind graph`
or `--kind semantic` (each takes its `--config`; the designs in
[`chronicle-hq/02-DESIGN/05-indexes.md`](../chronicle-hq/02-DESIGN/05-indexes.md)
carry the shapes), and are queried with `chronicle graph
neighbors|walk` and `chronicle semantic query`. Run `chronicle` with no
arguments for the full verb list.

**Optional: the semantic kind.** Semantic indexes need an
OpenAI-API-compatible `/embeddings` provider and stay unscheduled unless
one is configured — everything else works without it. A local server such
as [Ollama](https://ollama.com) works:

```sh
export CHRONICLE_EMBEDDING_API_KEY=...   # whatever the provider expects
chronicle up --embedding-url http://localhost:11434/v1 --embedding-model nomic-embed-text
```

The key reaches sandboxed workloads as a mounted file, never as
environment (decision
[0016](../chronicle-hq/03-DECISIONS/0016-the-semantic-kind.md)).

**Optional: the microsandbox backend.** By default workloads run in-process.
To run each placement in its own microVM instead, install the
[microsandbox](https://github.com/microsandbox/microsandbox) CLI (`msb`,
pinned at **0.6.8**) and hand `up` the linux guest workload for your
architecture — the `chronicle-workload_<version>_linux_arm64` or
`..._linux_amd64` artifact from the release page (guests are linux in the
host's architecture, no matter the host OS), or build both with
`make workload-linux`:

```sh
chronicle up --backend microsandbox --workload-binary ./chronicle-workload_0.1.0_linux_arm64
```

## The multi-host fleet

`chronicle up` is the getting-started and single-host shape. The same
fleet composes from the standalone binaries when you want the pieces
scheduled individually — control plane, per-tenant nodes, and one executor
per host, bidding for placements over the shared roster:

```sh
chronicle-control --dir <data-dir>
chronicle-node --url <nats-url> --creds <tenant-service-creds>
chronicle-executor --url <nats-url> --creds <control-creds> --id <stable-host-id> \
  --backend inprocess|microsandbox [--workload-binary <path>]
```

The fleet design
([`chronicle-hq/02-DESIGN/04-fleet.md`](../chronicle-hq/02-DESIGN/04-fleet.md))
and the scheduler design
([`06-scheduler.md`](../chronicle-hq/02-DESIGN/06-scheduler.md)) carry the
shape; all binaries ship in every release archive (decision
[0017](../chronicle-hq/03-DECISIONS/0017-release-flow.md)).

## Layout

| Path | What it is |
|---|---|
| `cmd/chronicle` | The CLI, and — through `chronicle up` — the whole local fleet in one process (thin main; logic in `internal/cli` and `internal/fleet`). |
| `cmd/chronicle-control` | The control plane, standalone: tenant minting, JWT issuance (thin main; logic in `internal/control`). |
| `cmd/chronicle-node` | One tenant's node, standalone: the fold, state, and API verbs (thin main; logic in `internal/node`). |
| `cmd/chronicle-executor` | The per-host executor: joins the roster, bids, runs placements on its backend (thin main; logic in `internal/executor`). |
| `cmd/chronicle-workload` | The one binary a placement runs — node or index kind — and the guest half of the microsandbox backend (thin main; logic in `internal/workloads`). |
| `contract` | The wire contract: subjects, headers, stream/bucket names, META grammar. |
| `client` | The public Go client package — the one way callers talk to the fleet. |
| `specs/` | The spec-kit increments this repo was built through. |

## Build & run from source

```sh
make check   # fmt + tidy + build + test + lint — the quality gate
make build   # all binaries land in bin/
```

## Releases

Pushing a `v*` tag builds and publishes a GitHub release via goreleaser
([release workflow](.github/workflows/release.yml)): one archive per
platform carrying the five binaries, plus the standalone linux guest
workloads (amd64 and arm64), with the tag's version stamped into every
binary (decision
[0017](../chronicle-hq/03-DECISIONS/0017-release-flow.md)). CI runs the
same gate as `make check` on every push and pull request. Rehearse locally
with `make snapshot`.

## License

[Sustainable Use License](LICENSE) — free for internal business,
non-commercial, and personal use.
