# Plan 022 — membership without custody

One increment on branch `25`, the open half first; the managed half
(`chronicle-service`, the same work id) pins the commit this lands as.

1. **Contract**: `ValidatePrincipalName`, `Roles`, `KnownRole`,
   `ServicePrincipal`. **Registry**: `Seed(…, admin, publicKey)`,
   `RequireRole` accepts the service principal.
2. **Client**: the two request/response pairs, `AddMember` with
   `WithPublicKey`/`WithGithubID`, `RevokeMember`, `ListMembers`.
3. **Node**: `members.go` — the two handlers; the endpoints registered
   beside the others.
4. **CLI**: the dispatch as a verb map; `Extension.Override`; the `member`
   sentences and their help section.
5. **Tests**: the node's member verbs end to end; the CLI's spine gains
   the member sentences; the extension test gains the override seam.

Gate: `make check` green.
