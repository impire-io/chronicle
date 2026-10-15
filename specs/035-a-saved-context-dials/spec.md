# Spec 035 — a saved context dials, for a solution's own CLI

**Work ID:** `chronicle-66` (tracker item chronicle-66, a task)
**Design:** `chronicle-hq` @ `a1449cc` — decision
[`0043`](../../../chronicle-hq/03-DECISIONS/0043-the-client-half-of-the-identity-bridge-is-open.md)
(the client half of the identity bridge is open),
[`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md)
§ contexts and the selection, and decision
[`0048`](../../../chronicle-hq/03-DECISIONS/0048-chronicle-agents-is-a-solution-built-on-chronicle.md)
(a solution's CLI speaks the account as a client).
**Status:** implemented on this branch.

## What this delivers

The open module kept the sign-in context's dial private: `chronicle
login` saves a context naming the install's bridge profile and the
account it landed in, and every account sentence dials through it — the
stored GitHub token refreshed when stale, the cached profile refetched
once on a refusal — but only from inside the `cli` package. A solution's
own CLI (`chronicle-agents store init`) could load that context
(`LoadContext` is exported) and not speak it; a hosted workspace was
initialised with an admin service account's credential instead.

- **`Context.Dial`.** The `cli` package's `Context` gains `Dial()
  (*client.Client, error)`: the way every account sentence connects —
  a sign-in context through its bridge into its account, an nkey with
  its principal, a credential file — with the same refusals the
  sentences give (a bridge without an account, a context that is nobody,
  an nkey without a principal, no url). `LoadContext` then `Dial` is a
  solution's whole connection; the store the context selected is the
  `Context`'s to read.
- **Where it lives.** The `cli` package, not `bridge` or `client`: the
  login state sits beside the profile under the CLI's config root, and
  refreshing it is the CLI's. `bridge` stays a client of the identity
  bridge; `client` stays the account's wire.

## Out of scope

- **Flags, environment and the quick start's fallback.** `Dial` takes
  the context as saved; `--url`, `--creds`, `CHRONICLE_STORE` and the
  dev dir's nkey are the chronicle CLI's own resolution and stay in it.
  A solution with flags of its own fills a `Context` and dials it.
- **The solution's side.** `chronicle-agents store init` picks this up
  in its own repo, at the open release that carries it.

## Requirements

- **FR-01 The sign-in form.** A `Context` with a bridge and an account
  dials through the bridge into the account as the sentences do (the
  tests observe the dial through `SetBridgeDial`); a bridge without an
  account is refused with a message naming `chronicle login`.
- **FR-02 The local forms.** A `Context` with a url and an nkey with its
  principal connects (the quick start's own context does, and lists
  stores); a credential file likewise; an nkey without a principal, a
  context with neither, or one without a url, is refused with the
  sentences' own words.
- **FR-03 The gate (quality).** `make check` green, race on.
