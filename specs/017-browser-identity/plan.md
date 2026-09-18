# Plan 017 — the browser identity bridge

## Layout

```
internal/mint/bootstrap.go        AUTH account + ensureAuthAccount (upgrade on
                                  load): external authorization {bridge issuer,
                                  allowed_accounts ["*"], xkey}, sentinel user;
                                  persists auth-issuer.nk, auth-xkey.nk,
                                  sentinel.creds; preloadAccounts gains AUTH
internal/identity/github/github.go Validator: ValidateToken(ctx, token) →
                                  {ID, Login}; DeviceFlow: Start/Poll; base
                                  URLs configurable, fakes in tests
internal/control/bridge.go        the $SYS.REQ.USER.AUTH responder: xkey
                                  decrypt → AuthorizationRequest → parse
                                  auth_token "<tenant>:<token>" → validate →
                                  loadTenant + registry lookup by GitHub ID →
                                  scoped user JWT, exp = min(TTL, token life)
                                  → AuthorizationResponse; registers only when
                                  a GitHub client ID is configured
internal/registry/registry.go     membership record gains the optional GitHub
                                  ID; lookup by it within one tenant
internal/control/members.go       MEMBER.ADD carries the optional binding
internal/cli/cli.go               chronicle login --bridge F (device flow,
                                  refresh stored 0600); --bridge on client
                                  verbs beside --creds
internal/fleet/fleet.go           --github-client-id on up; bridge profile
                                  emit beside tenant create outputs
```

## Order

1. Bootstrap: AUTH account, seeds, sentinel, upgrade path — unit +
   emitted-preload tests (spec 016's integration test gains AUTH for
   free once preloadAccounts grows).
2. GitHub package with fakes.
3. The responder, tested end to end against the embedded operator
   server with external auth enabled: sentinel + fake-GitHub token in,
   placed connection out; refusals for unknown tenant, unbound ID,
   revoked membership, expired token.
4. Registry + MEMBER.ADD binding.
5. login + --bridge client path.
6. `make check`; ready.

## Open at build time

- The AuthorizationRequest/Response claim shapes against the vendored
  nats-server 2.14 (ADR-26) — pin exactly what the embedded server
  accepts; the integration test is the authority.
- Whether `up` re-pushes AUTH on upgrade of a live dev dir or requires
  restart (restart acceptable for dev; note in output either way).
