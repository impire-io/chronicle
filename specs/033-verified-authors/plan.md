# Plan 033 — a request names its caller in its subject, and the node verifies an author

## Layout

```
contract/names.go               APIRoot, LiveRoot, APISubject, APIEndpoint, CallerFromSubject, MemberBaselinePublish/Subscribe
contract/record.go              Op-Attester, Op-Signature; Op.Attester/Signature/Verified; SignedForm, Attest, VerifyAttestation
contract/errors.go              not-the-caller, undefined-operation, undeclared-child, schema-violation, instance-exists, instance-moved
contract/judge.go               Decision Unverified
contract/metakeys.go            StoreConfig.VerifiedAuthors; MetaAttester, Attester
contract/logname.go             live reserved
contract/sdk.go                 ContractVersion 3.0.0
contract/sdk-contract.json      the artifact at 3.0.0
client/api.go                   subject functions and endpoints; no Principal field; ApplySubject, ParseApplySubject, ApplyResponse; WithVerifiedAuthors
client/ops.go                   Verified(); viaNode routing; applyThroughNode; Preflight, OpLanded, GuardRefused exported; typeCache.requiresVerified/verify
client/iter.go                  op.Verified set on every yielded operation
client/client.go                KeyPairFromSeedFile, KeyPairFromCredsFile
client/sdk_contract_test.go     the parity test at 3.0.0
registry/registry.go            RequireRole's premise restated
foldcore/pass.go                RequireVerified, Verify
foldcore/verify.go              Verifier, StoreRequiresVerified
node/node.go                    Config.Signer; endpoints on CHRON.API.*; requireCaller; recordAttester; the apply endpoint
node/apply.go                   handleApply
node/store.go, index.go, members.go   the caller from the subject
node/fold.go                    the author gate on the fold
index/projection/projection.go  RequireCaller; the author gate on state-sourced passes
index/{search,graph,semantic}/service.go   IndexQueryEndpoint; RequireCaller
up/up.go                        two users: the node's and the bound member's; the node's signer
devdir/devdir.go                NodeNkeyFile, NodeNkeyPath
cmd/chronicle-node, cmd/chronicle-workload   the signer from the creds or the seed
cli/store.go, cli/instances.go  --verified-authors, --verified, the VERIFIED column, store get
conformance/fixtures/attestation.json   the signed form and the verdict
conformance/fixtures_test.go    TestFixtureAttestation
conformance/scenarios/03-verified-authors.json   the live scenario
conformance/scenarios_test.go   verb.as, apply.direct, apply.as; verifiedAuthors, verified; expect.verified, verifiedEach, refused, no-state
AGENTS.md, README.md            the direct append path; the baseline for an operator's NATS
specs/033-verified-authors/
```

## Mechanics

1. The contract first: the grammar, the headers, the codes, the decision, the version — so the parity test drives the rest.
2. The client: subjects as functions of the caller, the request types without a principal, the verified route beside the direct one, the mark on read.
3. The node: every handler reads its caller from the subject; the verified write; the attester key at boot; the author gate on the fold. The indexers the same, through the projection.
4. The quick start: two users, the member bound by the baseline, the node signing.
5. The CLI's flags and column.
6. The artifact, the golden fixture, the live scenario.
7. `make check` green: fmt, tidy, build, race tests (the suite boots `up`), lint.
8. One PR, draft from the first push, labelled `work/23`, ready and merged once chronicle-hq #85 is in (it is); then the release tag `v0.7.0` (decision 0017), whose archive carries the 3.0.0 artifact and suite; chronicle-service, chronicle-js and chronicle-web follow in the item's `lands` order; chronicle-hq flips the designs to `implemented` when the contract runs on main.

## Measured

Verification on the fold: 35 µs per operation; signing at the node's door: 16 µs (Apple M3 Max, Go `crypto/ed25519`, 1 KiB payload) — chronicle-hq research 019.
