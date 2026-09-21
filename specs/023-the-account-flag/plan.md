# Plan 023 — the word is account

## Layout

```
cli/cli.go            --tenant → --account on connectFlags; resolve() takes the
                      context's bridge and account when no way is explicit;
                      dial() names both fixes when --bridge has no account;
                      context save --bridge F --account A; context show prints
                      them; Extension.BridgeDial(profile, account)
cli/context.go        storedContext + Context gain Bridge, Account; SaveContext
                      treats a bridge as one way of being someone
registry/registry.go  Seed(…, opts ...SeedOpt); WithGithubID
cmd/*/main.go         flag help says account
README.md, CONTRIBUTING.md, AGENTS.md   the product concept says account
```

## Order

1. The flag and the context fields, with the extension test extended: a
   bridge context dials with the profile and the account, `--account`
   beats the context's, `context show` prints both, `--bridge` alone
   teaches.
2. `registry.Seed` options — exercised by the managed service's onboarding
   test (spec 024), which seeds an admin bound by GitHub id.
3. The docs and help.
4. `make check`; the managed build pins this commit and lands after it.
