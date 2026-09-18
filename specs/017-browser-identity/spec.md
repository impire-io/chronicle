# Spec 017 — the browser identity bridge

**Work ID:** `08-browser-identity` (chronicle-hq design 08)
**Design:** `chronicle-hq` @ `e2a359a` —
[`02-DESIGN/08-browser-identity.md`](../../../chronicle-hq/02-DESIGN/08-browser-identity.md),
per decision
[`0026`](../../../chronicle-hq/03-DECISIONS/0026-browser-identity-is-a-callout-bridge-github-first.md)
(extends 0007; the verified callout facts are research 001 § 4, the
wildcard check research 006).
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge.

## What this delivers

A human logs in with GitHub and gets a real, placed NATS connection.
Nothing else about auth moves: `.creds` possession stays the only
machine identity, the registry stays the sole authority on membership
and role, and an install that configures no GitHub App simply has no
bridge.

1. **The AUTH account** joins the bootstrap beside SYS and CONTROL —
   external authorization naming control's bridge issuer,
   `allowed_accounts: ["*"]` (source-verified wildcard; the mint never
   re-pushes AUTH), xkey-encrypted requests. The bootstrap persists two
   new seeds (bridge issuer, xkey curve) and a **sentinel user** with no
   rights beyond triggering callout. Existing bootstrap dirs upgrade on
   load, the `ensureControlJetStream` way. Spec 016's preload seam
   carries AUTH into emitted cluster configs without a rendering change.
2. **The callout responder in control** answers `$SYS.REQ.USER.AUTH`:
   decrypt, read `auth_token` as `<tenant>:<github-token>` (the target
   tenant is always explicit), validate the token against GitHub, map
   the **numeric GitHub user ID** to a membership in that tenant's
   registry, and answer with a user JWT signed by the tenant's scoped
   member key — member baseline on the wire, role enforced at the API,
   `exp` = min(configured TTL, the GitHub token's remaining life),
   TTL default one hour. Every refusal is one uniform authentication
   error to the wire with the reason logged control-side only.
3. **The GitHub seam**: one GitHub App, install configuration
   (`--github-client-id` on `up`/control; absent means no bridge and the
   responder never registers). Token validation and the device flow live
   in one package with configurable endpoints — tests run against local
   fakes, never against github.com (the no-mocked-NATS rule's spirit:
   the NATS side of the bridge is tested against the real embedded
   server with a fake GitHub, both halves honest).
4. **Membership gains the binding**: `MEMBER.ADD` accepts an optional
   GitHub ID; the registry record carries it; the bridge matches on it.
   (Invites stay unbuilt per 0024 — binding at add is the first shape.)
5. **`chronicle login`** runs the device flow against a **bridge
   profile** — a small JSON handed out like a creds file (URL, GitHub
   client ID, sentinel creds inline) — and stores the refresh token
   0600 under the chronicle dir. Client verbs accept `--bridge F` to
   connect through the bridge instead of `--creds`: refresh → connect
   with sentinel + `tenant:token` → reconnect on expiry re-runs it. The
   panel is another client of the same flow, not a second one.

## Contract

- **Callout can only place, never provision** — a tenant or membership
  that does not exist is a refusal, and no registry write ever happens
  on the callout path.
- **The bridge is additive.** No change to `Op-Author` semantics, to
  `.creds` issuance, or to any existing verb's wire shape. `MEMBER.ADD`'s
  new field is optional and absent means today's behavior. One deliberate
  baseline extension, stated: members gain pub-allow on
  `$SYS.REQ.USER.INFO` (whoami — the reply names only the caller's own
  identity; the bridge dial reads its principal from it). New mints carry
  it; existing tenants gain it at their next rekey.
- **Custody does not move.** The bridge issuer and xkey seeds live in
  the bootstrap dir with everything else; signing stays in control.
- **Failure prices as designed**: GitHub down or control down blocks
  new human logins only; existing connections ride to expiry; machines
  never touch the bridge. Membership revocation bounds at the TTL.

## Out of scope

- Invites (by GitHub handle or otherwise) — layered on the invite flow
  when it lands (0024).
- A second identity provider; per-role wire enforcement; op signing.
- The websocket listener (design 07 names its moment: the panel's
  arrival) and the panel itself.
- Keyring storage for the refresh token — file 0600 first, keyring by
  demand.
