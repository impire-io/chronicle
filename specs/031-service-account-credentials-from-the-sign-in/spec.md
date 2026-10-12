# Spec 031 — a service account's credential, from the sign-in

**Work ID:** `chronicle-63` (tracker item; lands after chronicle-hq #75,
before chronicle-service, chronicle-js and chronicle-web)
**Design:** `chronicle-hq` @ `5c1d258` (hq PR #75) — decision
[`03-DECISIONS/0052-an-admin-issues-and-rotates-service-account-credentials-through-the-identity-plane.md`](../../../chronicle-hq/03-DECISIONS/0052-an-admin-issues-and-rotates-service-account-credentials-through-the-identity-plane.md),
amending [`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md)
§ members and service accounts and § two planes,
[`02-DESIGN/08-browser-identity.md`](../../../chronicle-hq/02-DESIGN/08-browser-identity.md)
§ the identity plane, and [`02-DESIGN/11-the-two-forms.md`](../../../chronicle-hq/02-DESIGN/11-the-two-forms.md)
§ membership.
**Status:** implemented on this branch.

## What this delivers

Hosted, `chronicle service-account create` registered the name and sent
the user to the operator's CLI for the credential — a sentence a
self-service admin cannot speak. Decision 0052 gives the identity plane
three verbs an admin of an account may speak about its service accounts:
issue, rotate, revoke. This repo carries the open half: the wire, the
client of it, and the sentences.

1. **The `bridge` package** gains the three subjects under the identity
   root — `SERVICE_ACCOUNT.ISSUE`, `SERVICE_ACCOUNT.ROTATE`,
   `SERVICE_ACCOUNT.REVOKE`, the identity's id and login as the last two
   tokens as for the two verbs before them — with their payloads
   (`ServiceAccountIssueRequest` `{account, name, role}`,
   `ServiceAccountRequest` `{account, name}`), their answers
   (`ServiceAccountCredential` `{account, name, role, public_key, creds}`,
   `ServiceAccountRevokeResponse` `{account, name, public_key}`) and the
   install's codes (`forbidden`, `not-a-service-account`,
   `credential-exists`, `not-a-member`). `Identity` speaks them:
   `IssueServiceAccount`, `RotateServiceAccount`, `RevokeServiceAccount`.
   The package stays a client: the mint, the revoke and the admin check
   that answer are the service's.
2. **The CLI's `service-account` noun** — `list | create NAME --role ROLE
   | rotate NAME | revoke NAME`, the design's grammar. With a sign-in
   context (a bridge profile and an account, as `login` saves) `create`,
   `rotate` and `revoke` reach the install as the signed-in identity, about
   the context's account; `create` and `rotate` show the credential
   exactly once — printed, or written to `--out FILE` with the file's
   path printed and the credential never. A refusal carries the install's
   own words. With a credential or an nkey (self-hosted) the sentences
   write the registry only, as before: `create` records `--public-key`,
   `rotate NAME --public-key KEY` retires the record and records the new
   key with the same role (the registry has no update verb), `revoke`
   retires it; each says the credential is your NATS's to issue or to
   kill.
3. **The seam** — `cli.IdentityPlane` and `SetIdentityDial` let a build
   that wraps the sentences, and the tests, stand in for the install.

## Tests

- `bridge`: the three subjects compose and read back the identity; the
  three methods speak the right subject and payload and read the answer
  and the refusal's code, against an embedded server with a plain
  responder standing in for the install.
- `cli`: the sign-in form shows the credential once, honors `--out`
  (0600, never printed), speaks about the context's account, closes the
  placement after each sentence, and surfaces a refusal; the self-hosted
  `rotate` records the new key with the role kept, refuses without
  `--public-key`, and refuses a person's name; the grammar and the
  vocabulary tests cover the new verb.

## What it does not do

No change to the SDK contract artifact or the conformance suite: the
identity plane stays outside `sdk-contract.json` until a bridge runs in the
conformance harness (decision 0043 point 4). No responder here — the
install answers (chronicle-service). No member issuance from the sign-in:
people sign in through the bridge. No release: the version is cut by hand
after this lands.
