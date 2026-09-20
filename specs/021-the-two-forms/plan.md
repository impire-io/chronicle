# Plan 021 — the two forms

One increment, one branch (`23`), landing first in the split's order —
the managed service pins the commit this lands as.

1. **Remove what leaves.** `internal/{mint,control,identity,executor,workloads,fleet}`,
   `cmd/{chronicle-control,chronicle-executor,chronicle-workloads}`, the
   managed CLI verbs and `login`, `contract/{fleet,bridge}.go`, specs
   005/006/007/017/018/020 — copied verbatim into the service's first
   content before they go.
2. **Make public what the service composes.** `git mv` out of
   `internal/`; rewrite the import paths; restate the seams in
   `.golangci.yml` (a `cli{,/**}` glob, because a bare `cli` prefix
   matches `client`).
3. **Divide the contract and the client.** `contract/placement.go`;
   `client.Request` exported; `Control` and the bridge dial gone;
   `ConnectWith`, `ConnectNkeyFile` added.
4. **The open `up`** and its supervisor; `registry.Seed`; `devdir`
   reduced to the quick start's conventions; `chronicle-node --admin`,
   `--nkey` on both open mains.
5. **The CLI's extension seam**, the runner, nkey contexts, the
   quick-start fallback; the tenant sentences unchanged.
6. **Tests**: the spine and the query test over the quick start; the
   context store in both ways; the extension's contract; `up`'s
   could-not-succeed-if-broken read.
7. **Release, CI, DCO, docs**: the three-binary matrix and formula, the
   DCO job, `CONTRIBUTING.md`, the README and agent guide for the open
   form.

Gate: `make check` green — format, tidy, build, race-detected tests, lint.
