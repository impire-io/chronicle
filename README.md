# chronicle

Chronicle is ops-logs as a product: append-only event logs whose folded
state and declared indexes (search, graph, semantic) run as NATS micro
services — for one tenant, on any NATS you already have. There is no web
UI and no side door: a CLI over the one client surface, a node that folds
your logs, one process per declared index, and a quick start that embeds
its own server for the five-minute path.

This repository is the **open tenant plane** of chronicle (chronicle-hq
design
[`11-the-two-forms.md`](../chronicle-hq/02-DESIGN/11-the-two-forms.md),
decision
[0031](../chronicle-hq/03-DECISIONS/0031-open-is-one-tenant-the-service-is-managed.md)):
everything that runs, or is used, inside one tenant. Creating tenants for
strangers, placing their workloads across hosts, running them in microVMs,
and logging humans in are the managed service at chronicle.impire.io,
built on this code in its own repositories. The repo exists by decision
[0010](../chronicle-hq/03-DECISIONS/0010-chronicle-repo.md) of
[`chronicle-hq`](https://github.com/impire-io/chronicle-hq) — the source
of truth for mission, research, designs, and decisions. Capabilities land
here through the build handoff
([playbook 04](../chronicle-hq/00-META/process/04-build-handoff.md)), not
by invention in this repo. Agents start at [AGENTS.md](AGENTS.md).

## Getting started

**1. Get the binaries.** One brew install gets the open set — `chronicle`
(the CLI, which also runs the quick start), `chronicle-node`, and
`chronicle-workload`:

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

**2. Run the quick start.**

```sh
chronicle up
```

One process: an embedded NATS server with JetStream, one account, one
user whose key lives under `~/.chronicle/dev` (change with `--dir`), the
node, and every index you declare — placed in the same process the moment
the node reports it. No operator, no JWTs, no ceremony; it listens on 4222
(`--port`, `-1` picks a free one) and runs in the foreground until
interrupted. It is a development convenience and says so: production is
your own NATS (below).

**3. Work with things** — in another terminal. The quick start's one user
is the tenant's admin, and every sentence finds it through the data dir,
so nothing needs a flag:

```sh
chronicle log create orders           # creates and selects the working log
chronicle type define invoice --def '{
  "schema": {"type":"object"},
  "operations": {
    "create":      {"schema": {"type":"object"}, "effect": "merge"},
    "comment.add": {"schema": {"type":"object","required":["body"]}},
    "status.set":  {"schema": {"type":"object"}, "effect": "merge"}
  }}'
chronicle create invoice.invoice-1 --payload '{"total":3}'
chronicle do invoice.invoice-1 comment.add --payload '{"body":"hi"}'
chronicle get invoice.invoice-1
chronicle history invoice.invoice-1
```

A *log* is an append-only event stream; a *type* is the vocabulary you
define on it — the thing's shape, its operations (each with a schema and
an effect), its aspects; a *thing* is one entity, its tail naming its
type (`invoice.invoice-1`). Creating is invoking the type's `create`
operation with the birth guard; `do` invokes any operation (payloads
failing the schema are refused before the wire); the node folds ops into
state; `get` reads the fold; `history` walks the ops. `chronicle type
init` prints a definition skeleton to start from, `chronicle op
define|list|inspect|rm` manage a type's operations one at a time, and
fully merge-covered things compact:

```sh
chronicle rollup invoice.invoice-1
```

**4. Declare indexes.** An index is declared on a log and served by a
process of its kind; search is the default kind, and one `query` verb
serves every kind — the index's declaration shapes the arguments:

```sh
chronicle index declare text
chronicle query text widgets
```

The graph and semantic kinds are declared the same way with `--kind graph`
or `--kind semantic` (each takes its `--config`; the designs in
[`chronicle-hq/02-DESIGN/05-indexes.md`](../chronicle-hq/02-DESIGN/05-indexes.md)
carry the shapes) and queried through the same verb — text for semantic,
`--from` (and `--depth` to walk) for graph. `chronicle log list`,
`chronicle index list`, `chronicle type list`, and `chronicle things`
say what exists. Run `chronicle` with no arguments for the full verb
list.

**Optional: the semantic kind.** Semantic indexes need an
OpenAI-API-compatible `/embeddings` provider and stay declared but
unserved unless one is configured — everything else works without it. A
local server such as [Ollama](https://ollama.com) works:

```sh
export CHRONICLE_EMBEDDING_API_KEY=...   # whatever the provider expects
chronicle up --embedding-url http://localhost:11434/v1 --embedding-model nomic-embed-text
```

## Bring your own NATS

Any NATS in any auth mode — a plain server with accounts in its config, an
operator-mode estate, a Synadia plan of any size. Chronicle asks for three
things: **one account**, **JetStream** on it, and users in it. Then:

- **The node and the indexers are your processes.** `chronicle-node
  --url U --creds F` (or `--nkey F`) with a user that has full rights in
  the account; on the account's first run add `--admin <principal>` to
  seed the membership registry with its admin. One `chronicle-workload
  --kind index-search --log L --index I` (graph, semantic) per declared
  index, as many instances of each as you want, under whatever
  supervises processes for you already. A declaration without a running
  process stays honestly unserved.
- **A member is a user with the baseline's rights**, which you grant
  however your server takes permissions — a `permissions` block in a
  config file, a scoped signing key in `nsc`, a Synadia team policy:

  | | Allow |
  |---|---|
  | publish | `CHRON.>`, `$SYS.REQ.USER.INFO`, `$JS.API.CONSUMER.>`, `$JS.API.STREAM.INFO.>`, `$JS.API.STREAM.NAMES`, `$JS.API.STREAM.MSG.GET.>`, `$JS.API.DIRECT.GET.>` |
  | subscribe | `CHRON.>`, `_INBOX.>` |

  Membership and roles live in the tenant's `META` bucket and the node
  enforces them; the principal a client acts as is the creds file's JWT
  name, or stated beside an nkey. Save it once and speak through it:

  ```sh
  chronicle context save prod --url tls://nats.example.com:4222 --creds dana.creds
  chronicle context save prod --url tls://nats.example.com:4222 --nkey dana.nk --principal dana
  chronicle context select prod
  ```

What this form cannot do, by construction: create a second tenant, place
a workload on another host, run anything in a microVM, log a human in
with GitHub, invite anyone. Those are the managed service.

## Layout

| Path | What it is |
|---|---|
| `cmd/chronicle` | The CLI, and — through `chronicle up` — the quick start in one process (thin main; logic in `cli` and `up`). |
| `cmd/chronicle-node` | One tenant's node, standalone: the fold, state, and API verbs (thin main; logic in `node`). |
| `cmd/chronicle-workload` | The placement binary: a node or an index kind as one process, whoever starts it — your unit, the quick start, or the managed executor's guest (thin main). |
| `contract` | The tenant wire contract: subjects, headers, stream/bucket names, META grammar, the placement kinds and the node's index report. |
| `client` | The public Go client package — the one way callers talk to a tenant, on any NATS in any auth mode. |
| `node`, `index/*`, `foldcore`, `registry` | The node, the three index kinds and their shared projection, the fold judgment, the membership registry. Public so the managed service composes them; the dependency runs one way. |
| `cli`, `up`, `devdir`, `guestnet` | The tenant sentences (with the seam a build adds verbs through), the quick start, its data-dir conventions, a guest's way to its host. |
| `specs/` | The spec-kit increments this repo was built through. |

## Build & run from source

```sh
make check   # fmt + tidy + build + test + lint — the quality gate
make build   # all binaries land in bin/
```

## Releases

Pushing a `v*` tag builds and publishes a GitHub release via goreleaser
([release workflow](.github/workflows/release.yml)): one archive per
platform carrying the three binaries, plus the standalone linux guest
workloads (amd64 and arm64), with the tag's version stamped into every
binary (decisions
[0017](../chronicle-hq/03-DECISIONS/0017-release-flow.md) and
[0031](../chronicle-hq/03-DECISIONS/0031-open-is-one-tenant-the-service-is-managed.md)).
CI runs the same gate as `make check` on every push and pull request.
Rehearse locally with `make snapshot`.

## Contributing

Under the Developer Certificate of Origin — every commit signed off —
see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache License 2.0](LICENSE) — chronicle-hq decision
[0033](../chronicle-hq/03-DECISIONS/0033-the-tenant-plane-is-apache-2.md):
the open tenant plane is licensed for anyone to use, embed, and sell on,
with the patent grant a platform's adopters ask for. Contributions are
under the Developer Certificate of Origin, licensed by the sign-off
([CONTRIBUTING.md](CONTRIBUTING.md)). The managed service and the
environment are private and carry no license.
