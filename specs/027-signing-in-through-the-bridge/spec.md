# Spec 027 — signing in through an install's identity bridge

**Work ID:** `chronicle-54` (tracker item; lands after chronicle-hq #56,
before chronicle-service)
**Design:** `chronicle-hq` decision
[`0043`](../../../chronicle-hq/03-DECISIONS/0043-the-client-half-of-the-identity-bridge-is-open.md),
amending [`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md),
[`08-browser-identity.md`](../../../chronicle-hq/02-DESIGN/08-browser-identity.md)
and [`11-the-two-forms.md`](../../../chronicle-hq/02-DESIGN/11-the-two-forms.md).
**Status:** implemented on this branch.

## What this delivers

`chronicle login` and an identity's own `account create` lived only in the
managed build, which never reaches the brew tap: a stranger who installed
chronicle with Homebrew could not sign up from the terminal. Neither verb
holds anything private — the install profile, the GitHub App's client ID
and the sentinel are public, and the device flow needs no client secret.

1. **The `bridge` package** — the client half of the identity bridge:
   - the install profile (`Profile`, `ParseSite`, `FetchProfile` — https
     from the site's own host only, a redirect off it refused, 64 KiB at
     most — and `DecodeProfile`), and `DefaultSite`,
     `https://chronicle.impire.dev`;
   - the connect token `<selector>:<github-token>` (`ConnectToken`,
     `SelectorIdentity`) and the identity plane's two subjects and payloads
     (`MembershipsSubject`, `AccountCreateSubject`, `IdentityFromSubject`);
   - `Connect` into an account and `ConnectIdentity` into the identity plane,
     the principal read back from the server's who-am-I;
   - GitHub's device flow and refresh with the client ID alone (`GitHub`).
2. **The open CLI** — `login [--site URL | --bridge F] [--account A]`, and
   `account create <name> [--bridge F]` through the identity plane; every
   account sentence's `--bridge F --account A` dials through the bridge in
   every build. `Extension.BridgeDial` goes: the dial is the open CLI's.
   `AccountCreateThroughBridge` and `SetProfileHTTPClient` are exported for
   a build that wraps `account`, and for its tests.
3. **The seams** — `bridge` imports `client` and `contract` only (a
   depguard rule says so); the agent guide says the open module may speak
   an install's bridge as a client, and still mints nothing.

## Contract

- **Nothing private enters the open module.** The callout, custody,
  minting and credential issuance stay the managed service's; the client
  secret is never sent — refreshing a device-flow token needs none (GitHub:
  *"Required unless the user access token was generated using the device
  flow"*).
- **Tested here:** the site and profile rules, the fetch's refusals, the
  subjects round-tripping the identity, the device flow through a pending
  poll to the grant and the refresh against a fake GitHub (asserting no
  secret is sent), the choice among accounts, the cached profile naming its
  site, the login state's move, and `account create` teaching `login`.
- **Tested end to end in chronicle-service:** its login, published-profile,
  websocket and identity-plane tests drive this package against a real
  control and callout and a fake GitHub.

## Out of scope

- The identity plane in `sdk-contract.json` and the conformance suite — it
  needs a bridge in the harness; its own item (0043 point 4).
- The web flow (PKCE, the code exchange through control) — the console's,
  in chronicle-js.
