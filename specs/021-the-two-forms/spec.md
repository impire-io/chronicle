# Spec 021 — the two forms: the public repo keeps the tenant plane

**Work ID:** `23` (tracker item chronicle-23)
**Design:** `chronicle-hq` @ `cfd952e` —
[`02-DESIGN/11-the-two-forms.md`](../../../chronicle-hq/02-DESIGN/11-the-two-forms.md),
per decision
[`0031`](../../../chronicle-hq/03-DECISIONS/0031-open-is-one-tenant-the-service-is-managed.md)
(open is one tenant on your NATS; the service is managed, and private).
**Status:** implemented on this branch ([plan.md](plan.md)).

## Constitution check

Against `AGENTS.md` and `how-we-build`: no subject, header, stream, or
bucket a tenant speaks moves; the one contract change is a division of the
`contract` package — what only the managed service speaks leaves for its
own `wire` package, what a node speaks (the placement kinds, its index
report) stays; nothing enters the append path; every test runs against a
real embedded server; the quality gate is unchanged. No conflict.

## What this delivers

After this spec the repository holds the **open tenant plane** and
nothing of the managed service: everything that runs, or is used, inside
one tenant, on any NATS the operator already has.

1. **The line, as a module boundary.** Minting, control, the bridge, the
   executor and its backends, the workload service, the fleet composition,
   and the managed CLI verbs leave for `chronicle-service`, with specs
   005, 006, 007, 017, 018, and 020. The packages the service composes —
   `node`, `index/*`, `foldcore`, `registry`, `guestnet`, `devdir`, `cli`
   — leave `internal/` and become public; `.golangci.yml` restates the
   seams between them. `client` loses `Control` and exports `Request`,
   the one building block a build that adds verbs speaks the same way.
2. **The contract divides.** `contract/placement.go` keeps the placement
   kinds and the node's index report on `CHRONX.FLEET.REPORT` — a subject
   an open component speaks stays in the open contract even when only the
   service listens. The rest of the fleet wire is the service's.
3. **Any NATS, any auth mode.** `client.ConnectWith` and
   `client.ConnectNkeyFile` dial however the operator's server takes a
   user, the principal stated by the caller; the registry's role check is
   the trust tier, as it always was. `chronicle-node` and
   `chronicle-workload` take `--creds` or `--nkey`; `chronicle-node
   --admin` seeds a fresh account's registry (`registry.Seed`). Contexts
   hold a creds file or an nkey with its principal.
4. **The open `up`.** Package `up`: an embedded server with one account
   and one nkey user under `~/.chronicle/dev`, the registry seeded with
   `admin`, the node, and an in-process supervisor that answers the node's
   own index reports by starting the declared kind — search, graph,
   semantic with a provider — and stopping it on deletion. The node's
   boot re-derivation brings every declared index back on the next `up`.
   The tenant sentences find the running quick start through `--dir`
   with nothing saved.
5. **The CLI's seam.** `cli.RunWith` takes an `Extension`: extra
   top-level verbs, their help sections, and a bridge dial. The open
   binary adds `up`; the managed build adds its verbs and ships the same
   binary as a superset. An extension may not shadow the open grammar;
   `--bridge` without a dial is refused with the reason.
6. **The release set is the open set** — `chronicle`, `chronicle-node`,
   `chronicle-workload`, plus the standalone linux guest workloads — and
   the brew formula installs it. **Contributions are under the DCO**:
   `CONTRIBUTING.md`, and CI refuses a pull request whose commits lack the
   sign-off.

## Contract

- **The tenant wire is unchanged.** Every `CHRON.>` subject, header,
  stream, bucket, and META key a tenant touches is what it was.
- **The open quick start serves one tenant end to end** — tested: a
  folded state read and a served search index with nothing minted; a
  non-member's assertion refused on the same key; the index retired on
  deletion and placed again on re-declaration; the identity and the
  declared index surviving a restart of the same dir.
- **The CLI's day in the life runs over the quick start** — tested with
  the local user through `--dir` and through a saved nkey context.
- **Nothing here imports the managed service**, and the lint rules deny
  the seams inside the tenant plane.

## Out of scope

- The node's `member add|revoke` verbs (design 11 § membership, without
  custody) — a follow-up on the tracker; the quick start has its one
  admin, and a bring-your-own-NATS account is seeded with one.
- The license: it stays the Sustainable Use License (chronicle-hq
  decision 0033, closing the question 0031 deferred).
- The managed service's own shape and release — `chronicle-service`'s
  first content, landing after this.
