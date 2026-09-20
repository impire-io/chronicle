# Spec 022 — membership without custody: the node writes the registry

**Work ID:** `25` (tracker item chronicle-25)
**Design:** `chronicle-hq` @ `9419368` —
[`02-DESIGN/11-the-two-forms.md`](../../../chronicle-hq/02-DESIGN/11-the-two-forms.md)
§ membership, without custody, per decision
[`0031`](../../../chronicle-hq/03-DECISIONS/0031-open-is-one-tenant-the-service-is-managed.md);
the registry's content is
[`02-DESIGN/01-onboarding.md`](../../../chronicle-hq/02-DESIGN/01-onboarding.md)
§ identity, its key grammar
[`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md).
**Status:** implemented on this branch ([plan.md](plan.md)); the managed
half lands in `chronicle-service` after it.

## Constitution check

Against `AGENTS.md` and `how-we-build`: two API verbs join
`CHRON.API.>` — `MEMBER.ADD` and `MEMBER.REVOKE`, served by the node with
the registry role check like every other verb; no subject, header, stream,
bucket, or META key a tenant touches changes shape; nothing enters the
append path; every test runs against a real embedded server. No conflict.

## What this delivers

The registry — who is a member, in which role — lives in the tenant's
META bucket and the node enforces it. After this spec the node is also the
one place that **writes** it, in both forms.

1. **The node's member verbs.** `CHRON.API.MEMBER.ADD` records a
   membership (role admin to call; the member's role one of admin, writer,
   reader, writer when unsaid; the principal record create-if-absent as
   the durable identity, the membership Create as the dedup gate;
   refusals `member-exists`, `bad-role`, `bad-principal-name`).
   `CHRON.API.MEMBER.REVOKE` retires one (the record goes, the principal
   record stays, the reply names the public key the membership held;
   `not-a-member`). Roles are what the API verbs enforce from the next
   request on; the credential is the issuer's to kill — the operator's
   NATS in the open form, the service in the managed one.
2. **The client and the CLI.** `client.AddMember`, `RevokeMember`, and
   `ListMembers` (a read of data at rest); the open sentences `chronicle
   member add <principal> [--role] [--public-key] [--github-id]`,
   `member revoke <principal>`, `member list`, spoken through a tenant
   admin's context. The CLI's dispatch is a verb map, and an `Extension`
   may now `Override` an open verb with a handler that delegates to it —
   how the managed build keeps its `member add <tenant> <principal>`
   beside the open sentence in one binary.
3. **The grammar and the vocabulary in the contract.**
   `contract.ValidatePrincipalName` (the identifier grammar, the service
   name reserved), `contract.Roles` and `KnownRole`, and
   `contract.ServicePrincipal` — the principal the tenant's own service
   acts as: never a member, accepted by the registry's role check as the
   operator, at the trust tier every asserted principal already has
   (0007: op signing by demand).
4. **One seed.** `registry.Seed(ctx, js, admin, publicKey)` births a
   registry with its admin — the quick start, `chronicle-node --admin`,
   and the managed mint all call it; it is the one write that does not go
   through a verb, because no node is placed yet.
5. **The managed verbs compose** (`chronicle-service`): control issues the
   credential under the tenant's scoped key, verifies it by connecting,
   then calls the node's `MEMBER.ADD` as the service principal with the
   public key and the GitHub binding; revoke reads the membership, kills
   the credential on the wire, then calls `MEMBER.REVOKE`; rekey rotates
   the signer and re-records every member through revoke and add with the
   new key. Control writes tenant META nowhere else.

## Contract

- **Roles gate verbs, not appends** — tested: a writer rolls up and
  cannot create a log; a reader can do neither; a revoked member is
  refused everything; a re-added principal is the same principal in its
  new role; the service principal holds every role and no registry entry.
- **The registry is the dedup gate** — tested: a second add of one
  principal is `member-exists`; a role outside the vocabulary
  `bad-role`; a name outside the grammar, or the reserved service name,
  `bad-principal-name`.
- **The open sentences run over the quick start** — tested in the CLI's
  day in the life: the admin adds a member who speaks through a context
  on the same seed under their own name, is listed, and is refused once
  revoked.
- **The managed lifecycle is unchanged for its callers** — the service's
  own suite (mint, add, revoke, rekey, the bridge's lookup) passes over
  the new path.

## Out of scope

- Killing a credential in the open form: the operator's NATS's business,
  by design.
- Invites, and per-role wire enforcement — 0007 left both where they
  were.
