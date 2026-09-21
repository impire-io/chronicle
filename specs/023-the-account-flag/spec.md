# Spec 023 — the word is account: `--account`, and a context that carries a login

**Work ID:** `chronicle-36` (tracker item; lands after chronicle-hq, before chronicle-service and chronicle-ops)
**Design:** `chronicle-hq` @ `091da42` —
[`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md)
(§ contexts and the selection, § self-service, in the CLI) and
[`02-DESIGN/11-the-two-forms.md`](../../../chronicle-hq/02-DESIGN/11-the-two-forms.md)
(§ the managed CLI), as amended by decision
[`0035`](../../../chronicle-hq/03-DECISIONS/0035-the-unit-is-the-account-onboarding-is-self-service.md)
(the unit is the account; onboarding is self-service through GitHub; the
plan lives in the account JWT). The designs flipped to `in-progress` for
this build at `chronicle-hq` @ `a63cd96`.
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge; the managed build (chronicle-service, spec 024) extends
this binary and lands after it.

## What this delivers

The open CLI says *account* where it spoke to the product, and a context
can carry a bridge login, so the managed build's `chronicle login` ends
inside an account without a flag on every sentence after.

1. **`--tenant` becomes `--account`** on every account-plane verb: the
   account a `--bridge` dial lands in. The help and the errors say account.
2. **A context carries a bridge login.** The stored context gains two
   optional fields, `bridge` (the profile path) and `account`, a third way
   of being someone beside a creds file and an nkey seed:
   `chronicle context save <name> --bridge F --account A`, and
   `context show` prints both. Resolution stays field-wise (0025): an
   explicit `--bridge`/`--account` beats the context's; with nothing
   explicit, the selected context's bridge and account serve, so every
   account sentence runs from a context `chronicle login` saved.
   `--bridge` with no account anywhere is a teaching error naming both
   fixes.
3. **The extension's `BridgeDial` takes `(profile, account)`** — the
   managed build's dial, unchanged in shape, renamed in meaning.
4. **`registry.Seed` takes options**: `registry.WithGithubID(id)` binds the
   seeded admin to a GitHub identity, so the managed service's mint of an
   identity's own account seeds its admin bound by its numeric id, holding
   no issued credential (0035 point 2). Absent, nothing changes.
5. **The docs and help say account** where they said tenant for the product
   concept: the README, CONTRIBUTING, AGENTS, the binaries' flag help
   ("the account's service-user credentials").

## Contract

- **No wire change.** Every `CHRON.>` subject, header, stream, bucket, and
  META key is what it was; the registry record's shape is unchanged (the
  GitHub id field existed since spec 017).
- **Internal identifiers keep the old word** — package and type names,
  the custody key prefix on the managed side, test names, and the decision
  files' paths (`0031-open-is-one-tenant…`, `0008-tenant-data-plane…`).
  Decision 0035 point 1 defers them to their own item, because the live
  bucket holds records under the old key and renaming them is a migration.
- **A stored context from before this spec reads unchanged**: the new
  fields are optional and absent means a creds or nkey context, as before.

## Out of scope

- The managed verbs' rename (`account create`, `member add <account> …`,
  `login` ending in a context) — chronicle-service, spec 024.
- The invite flow; a second identity provider.
