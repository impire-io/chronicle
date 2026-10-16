# Spec 033 — a role is resolved: META by default, the environment's responder by callout

**Work ID:** `chronicle-77` (tracker item; lands after chronicle-hq #96)
**Design:** `chronicle-hq` @ `e60b2fb` (hq PR #96) — decision
[`03-DECISIONS/0061-a-role-is-resolved-meta-by-default-the-environments-responder-by-callout.md`](../../../chronicle-hq/03-DECISIONS/0061-a-role-is-resolved-meta-by-default-the-environments-responder-by-callout.md),
amending [`02-DESIGN/02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md)
§ subject grammar, [`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md)
§ the identity slice, [`02-DESIGN/04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md)
§ the node's duties, [`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md)
§ members and service accounts and [`02-DESIGN/11-the-two-forms.md`](../../../chronicle-hq/02-DESIGN/11-the-two-forms.md)
§ membership.
**Status:** implemented on this branch.

## What this delivers

Decision 0061 makes every role check a question to one resolver: the
membership registry in META by default — the managed form and the quick
start, unchanged — or, where the operator's platform already keeps its
members and their roles, the environment's own responder on the account
wire. Chronicle is being installed inside such a platform; its team
implements the responder against this contract.

1. **`registry.Resolver`** — `Role(ctx, principal) (role, error)`, with
   `ErrNotAMember` for a principal the account does not hold and
   `ErrRoleSourceUnavailable` for a question that could not be answered.
   `registry.Require` replaces `RequireRole`: the same check every
   API-serving component runs, over whichever resolver it was started
   with; the service principal is the operator under both and is asked of
   nobody. `registry.Code` maps a refusal to its wire code.
2. **Two resolvers.** `registry.Meta(kv)` reads the identity slice as
   decision 0007 rules. `registry.Callout(nc, timeout)` requests
   `CHRON.ROLE.<principal>` with an empty payload and takes the reply —
   `contract.RoleReply`, `{"role":"admin"|"writer"|"reader"}` for a
   member, `{}` for anyone else. A role outside the vocabulary, a reply
   that does not decode, no responder, and no reply within the bound
   (`DefaultCalloutTimeout`, two seconds) are each
   `ErrRoleSourceUnavailable`: the request is refused with the source
   named, never answered from META. `registry.ForSource` turns the flag
   value — `meta` or `callout` — into a resolver.
3. **The contract.** `contract.RoleRoot`, `RoleSubject`,
   `RoleCalloutFilter` (`CHRON.ROLE.>`, what the responder subscribes to)
   and `RoleReply`; `role` joins `ReservedStoreNames`; two refusal codes
   join the catalog — `role-source-unavailable` on every interaction that
   checks a role, `membership-elsewhere` on `member.add` and
   `member.revoke`. The artifact and the names fixture follow; the
   version is bumped by a minor. No verb and no shape changes.
4. **The node.** `node.Config.Roles` — nil means META over the node's own
   handle. Every verb's check goes through the resolver; a refusal
   carries `registry.Code`. Under any resolver but the default the member
   verbs are refused with `membership-elsewhere` before anything is
   touched: membership is managed where the roles live.
5. **The index kinds.** `projection.Config.Roles`, passed through
   `search.Config`, `graph.Config` and `semantic.Config`; the shared
   `RequireCaller` becomes the projection's own method over its resolver.
6. **The binaries.** `--roles meta|callout` on `chronicle-node` and
   `chronicle-workload` (every kind), default `meta`; `--admin` beside
   `--roles callout` is refused at boot as a contradiction. `up` and the
   managed service's compositions set nothing and keep the default.
7. **The README** tells the self-hosting operator how to run the
   responder — the contract the other platform's team is handed.

## Tests

- `registry`: the META resolver answers from the slice and not-a-member
  past it; the callout takes a role and a non-member from a test
  responder, fails closed on a role outside the vocabulary, a reply that
  does not decode, a silent responder and no responder at all, within
  its bound; `Require` admits by role, refuses by role and by absence,
  never asks about the service principal, and `Code` maps each refusal;
  `ForSource` names the two sources and refuses a third.
- `node`: a node under the callout resolver asks the responder for every
  caller and reads the registry not at all — the registry's seeded admin
  is what the responder says, a principal the registry never heard of is
  the responder's admin, a stranger is refused, the member verbs are
  refused with `membership-elsewhere`, the service principal is the
  operator without a question asked, and silence is
  `role-source-unavailable`.
- `index/search`: the query endpoint asks the resolver the kind was
  started with — an in-process one, the embedder's seam — and refuses
  with the source's code when it does not answer.
- `conformance`: `role` is a reserved store name; the artifact test
  proves the reserved names and the error catalog equal the package's.

## What it does not do

No cache in front of the callout: the default reads META per request
today, and a responder may answer as fast as it likes. No recording of
the choice in META: each process is started with its own flag, and a
process left on the default in a callout account refuses callers the
registry does not hold, visibly. No change to `member list`: it reads the
registry through JetStream as every list does, and under callout it shows
what was seeded, which `--admin` refuses to be. No responder shipped: the
environment's is its own, in any language. No change to the managed
service: `chronicle-service` composes the default and does not move. No
release: the version is cut by hand after this lands.
