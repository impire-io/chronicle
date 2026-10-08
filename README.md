# chronicle

Chronicle keeps the full history of everything your service does — and
gives you the current state, search and relationships derived from it,
without building any of it. You define **types**, each with a schema and
the **operations** it allows; you create **instances** of those types;
every change is an operation applied to an instance and kept as its
**history**; the current **state** is always one read away; the
**indexes** you declare (search, graph, semantic) are kept current from
the history. It runs on any NATS you already have, or hosted at
chronicle.impire.dev — one product, a connection string apart.

This repository is the **open account plane** of chronicle (chronicle-hq
design
[`11-the-two-forms.md`](../chronicle-hq/02-DESIGN/11-the-two-forms.md),
decision
[0031](../chronicle-hq/03-DECISIONS/0031-open-is-one-tenant-the-service-is-managed.md)):
everything that runs, or is used, inside one account — the CLI, the Go
client, the node that keeps the state, one process per declared index,
and a quick start that embeds its own server. Creating accounts for
strangers, placing their workloads across hosts, and the identity bridge
that signs humans in are the managed service at chronicle.impire.dev,
built on this code in its own repositories; this CLI signs in to it
(`chronicle login`, decision
[0043](../chronicle-hq/03-DECISIONS/0043-the-client-half-of-the-identity-bridge-is-open.md)).
The repo exists by decision
[0010](../chronicle-hq/03-DECISIONS/0010-chronicle-repo.md) of
[`chronicle-hq`](https://github.com/impire-io/chronicle-hq) — the source
of truth for mission, research, designs, and decisions; the words it uses
are the [vocabulary](../chronicle-hq/00-META/vocabulary.md) of decision
[0044](../chronicle-hq/03-DECISIONS/0044-chronicle-speaks-the-users-language.md).
Capabilities land here through the build handoff
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
node, and every index you declare. It saves and selects a context named
`local` for that user, so every sentence below works without a flag. No
operator, no JWTs, no ceremony; it listens on 4222 (`--port`, `-1` picks
a free one) and runs in the foreground until interrupted. It is a
development convenience and says so: production is your own NATS
(below).

**3. The five-minute path** — in another terminal. The block below is
run as a test in CI, line by line, so it works as written:

```sh quick-start
chronicle store create orders                    # creates the store and selects it
chronicle type init invoice > invoice.yaml       # a commented type file to edit
chronicle type create invoice -f invoice.yaml
chronicle op create invoice send --schema '{"type":"object","required":["to"]}' --effect merge
chronicle instance create invoice/inv-1 --data '{"total":120}'
chronicle instance apply invoice/inv-1 send --data '{"to":"x"}'
chronicle instance list --type invoice
chronicle instance get invoice/inv-1
chronicle instance history invoice/inv-1
chronicle instance snapshot invoice/inv-1
chronicle index create text --kind search
chronicle index query text x
```

The grammar is one shape, noun then verb: `store`, `type`, `op`,
`instance`, `index`, `member`, `service-account`, `context`, `account`,
and every noun has `list`, `get`, `create` and `delete` where they mean
something. `--help` works on every command, `chronicle help <noun>` lists
a noun's verbs, and every list is a table (`--output json|jsonl|yaml` for
tools).

A **store** holds types, instances and indexes. A **type** says what an
instance looks like (a JSON Schema), which **operations** it allows (each
with a schema for its data and an **effect**: `merge` updates the state,
`none` is recorded in history only), which **children** may be nested
under it (`invoice/inv-1/comments/c-3`), and its **history policy**
(`compactable`, or `full` to keep every operation). An **instance** is
named by its **path**, `type/id`; `instance create` applies the type's
`create` operation, `instance apply` any other; the data is checked
against the operation's schema before it is sent. `instance get` reads
the state, `instance history` the operations, `instance list --type
invoice --where status=sent` the instances of a type with a filter, and
`instance snapshot` writes the current state as a snapshot and compacts
the history before it. `instance apply … --expect SEQ` refuses the write
if the instance moved past the sequence you read.

**4. Declare indexes.** An index is declared on a store and served by a
process of its kind; it reads the state (or, with `--source history`,
every operation) and is kept current:

```sh
chronicle index create rel --kind graph --config '{"edges":[{"field":"customer"}]}'
chronicle index query rel --from invoice/inv-1            # neighbours
chronicle index query rel --from invoice/inv-1 --depth 2  # a walk
chronicle index create meaning --kind semantic
chronicle index query meaning "orders about widgets"
```

Hits stream as they arrive, `--limit` caps them, and the total closes the
listing. The shapes of each kind's `--config` are in
[`chronicle-hq/02-DESIGN/05-indexes.md`](../chronicle-hq/02-DESIGN/05-indexes.md).

**5. Build on it.** The Go client is the SDK's Go form: instances by path,
collections as iterators, single state as a reply, and the live surface
— `Tail`, `Watch`, `WatchDeclarations` — fed by JetStream, never a core
subscription. What every SDK restates is described once in
[`contract/sdk-contract.json`](contract/sdk-contract.json) and checked by
the [conformance suite](conformance/README.md), which every release
archive carries (the SDK contract,
[`chronicle-hq/02-DESIGN/12-the-sdk-contract.md`](../chronicle-hq/02-DESIGN/12-the-sdk-contract.md)).

**Optional: the semantic kind.** Semantic indexes need an
OpenAI-API-compatible `/embeddings` provider and stay declared but
unserved unless one is configured — everything else works without it. A
local server such as [Ollama](https://ollama.com) works:

```sh
export CHRONICLE_EMBEDDING_API_KEY=...   # whatever the provider expects
chronicle up --embedding-url http://localhost:11434/v1 --embedding-model nomic-embed-text
```

**Optional: a browser.** A browser speaks NATS only over a websocket.
`--websocket-port` opens one on loopback, in the clear, for the same one
user; `--websocket-origin` (repeatable) limits which pages may open it.
The URL is printed and recorded as `websocket.url` in the data dir:

```sh
chronicle up --websocket-port 9222 --websocket-origin http://localhost:3000
```

## Bring your own NATS

Any NATS in any auth mode — a plain server with accounts in its config, an
operator-mode estate, a Synadia plan of any size. Chronicle asks for three
things: **one account**, **JetStream** on it, and users in it. Then:

- **The node and the indexers are your processes.** `chronicle-node
  --url U --creds F` (or `--nkey F`) with a user that has full rights in
  the account; on the account's first run add `--admin <principal>` to
  seed the registry with its admin. One `chronicle-workload
  --kind index-search --store S --index I` (graph, semantic) per declared
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

  Members and service accounts, with their roles, live in the account
  and the node enforces them (`chronicle member add`, `chronicle
  service-account create`); the principal a client acts as is the
  credential file's name, or stated beside an nkey. Save the connection
  once and speak through it:

  ```sh
  chronicle context add prod --url tls://nats.example.com:4222 --creds dana.creds
  chronicle context add prod --url tls://nats.example.com:4222 --nkey dana.nk --principal dana
  chronicle context select prod
  ```

What this form cannot do, by construction: create a second account, place
a workload on another host, run anything in a microVM, run the bridge that
signs a human in with GitHub, invite anyone. Those are the managed service.

## Sign in to the hosted service

The same CLI signs in to chronicle.impire.dev with your GitHub account —
no credential file, no operator:

```sh
chronicle login                       # GitHub's device flow; ends inside your account, created at the first sign-in
chronicle store create orders         # the account sentences run from the context login saved
chronicle account create side-project # a further account of your own, as the plan allows
```

`--site https://…` signs in to another install that runs a bridge. The
install profile is fetched over HTTPS from the site and cached beside your
contexts; the GitHub sign-in is kept beside it, readable by you alone, and
`chronicle logout` forgets it.

## Layout

| Path | What it is |
|---|---|
| `cmd/chronicle` | The CLI, and — through `chronicle up` — the quick start in one process (thin main; logic in `cli` and `up`). |
| `cmd/chronicle-node` | One account's node, standalone: the fold, state, and API verbs (thin main; logic in `node`). |
| `cmd/chronicle-workload` | The placement binary: a node or an index kind as one process, whoever starts it — your unit, the quick start, or the managed executor's guest (thin main). `--creds` or `--nkey` for the user, `--url` for the server; a guest that reaches a TLS server through an address its certificate cannot name adds `--tls-server-name` — the name is verified, never skipped. |
| `contract` | The account wire contract: subjects, headers, stream/bucket names, META grammar, the path grammar, the placement kinds and the node's index report. |
| `client` | The public Go client package — the one way callers talk to an account, on any NATS in any auth mode. |
| `bridge` | The client half of an install's identity bridge: the install profile, GitHub's device flow, the bridge and identity-plane connections. |
| `node`, `index/*`, `foldcore`, `registry` | The node, the three index kinds and their shared projection, the fold judgment, the membership registry. Public so the managed service composes them; the dependency runs one way. |
| `cli`, `up`, `devdir`, `guestnet` | The account sentences (with the seam a build adds verbs through), the quick start, its data-dir conventions, a guest's way to its host. |
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

[Sustainable Use License](LICENSE) — free for internal business,
non-commercial, and personal use; the fair-code posture every impire
product carries (chronicle-hq decisions
[0017](../chronicle-hq/03-DECISIONS/0017-release-flow.md) and
[0033](../chronicle-hq/03-DECISIONS/0033-the-public-repo-stays-under-the-sustainable-use-license.md)).
"Open" in the split's sense names the boundary — everything that runs
inside one account, published here — not an OSI license. Contributions
are under the Developer Certificate of Origin
([CONTRIBUTING.md](CONTRIBUTING.md)).
