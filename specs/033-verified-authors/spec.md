# Spec 033 — a request names its caller in its subject, and the node verifies an author

**Work ID:** `23` (chronicle-hq tracker item 23; the build of chronicle-hq decision 0057, first repo in the landing order)
**Design:** `chronicle-hq` @ `9f1d34b` (hq PR #85) —
[`03-DECISIONS/0057-a-request-names-its-caller-in-its-subject-and-the-node-verifies-an-author.md`](../../../chronicle-hq/03-DECISIONS/0057-a-request-names-its-caller-in-its-subject-and-the-node-verifies-an-author.md),
out of research [`019-op-authorship`](../../../chronicle-hq/01-RESEARCH/019-op-authorship/00-overview.md), and the amended designs
[`01-onboarding.md`](../../../chronicle-hq/02-DESIGN/01-onboarding.md) § creating an account, § `Op-Author` and what it is worth;
[`02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md) § subject grammar, § the operation record, § the write path;
[`03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md) § the META bucket, § store configuration, § the identity slice, § the state buckets;
[`04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md) § the node's duties;
[`07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md) § the grammar, § instances;
[`12-the-sdk-contract.md`](../../../chronicle-hq/02-DESIGN/12-the-sdk-contract.md) § the caller and the verified write, § the conformance suite.
**Status:** built on branch `23` ([plan.md](plan.md)).

## What this delivers

The account wire gains a caller, and an author a reader can rely on. Contract **3.0.0** — a changed meaning of names, major, once.

1. **Every verb carries its caller in its subject.** `CHRON.API.<principal>.<VERB…>`: the node and the indexers serve `CHRON.API.*.<VERB…>`, read the caller from the subject (`contract.CallerFromSubject`) and check its registry role; the `principal` field leaves every request type. The service principal speaks on `CHRON.API.chronicle.>`. The client's subject constants become functions of the caller (`client.StoreCreateSubject(principal)`, `client.IndexQuerySubject(principal, store, index)`); the endpoints a responder serves are beside them.
2. **The member baseline is one definition, enumerated.** `contract.MemberBaselinePublish(principal)` and `contract.MemberBaselineSubscribe()`: every store's operations, `CHRON.LIVE.>`, the verbs under the member's own name, the whoami request, the read half of the JetStream API; subscribe to the operations, the ephemeral namespace and inboxes — never the verbs. The managed service renders it with `{{name()}}`; the quick start renders it literally for its member; the README states it for an operator's own NATS. `live` joins the reserved store names (decision 0055's deferred line).
3. **The verified write.** `CHRON.API.<principal>.APPLY.<store>.<tail>`: a request carrying the record exactly as a direct publish carries it. The node reads the caller from the subject, refuses a record naming another author (`not-the-caller`), runs the client's pre-flight (`client.Preflight`, exported) and refuses with `undefined-operation`, `undeclared-child`, `schema-violation`, signs the record, publishes on the instance's exact subject with the caller's `Nats-Msg-Id`, guard and rollup scope, and answers `{seq}`. A guard miss is judged as the SDK judges it (`client.OpLanded`, exported) and answered as `instance-exists` or `instance-moved`.
4. **The attestation.** Two headers, `Op-Attester` and `Op-Signature`: the node's public key and its Ed25519 signature over `contract.Op.SignedForm()` — subject, message id, the five record headers and the payload's SHA-256. The node signs with the key it connects with (`node.Config.Signer`, from the creds or the seed in both binaries and in `up`) and records its public key at META `identity.attester.<pub>` at boot, create-if-absent. `Op-Version` stays `1`.
5. **A store may require verified authors.** `StoreConfig.VerifiedAuthors`, `store create --verified-authors`, `client.WithVerifiedAuthors()`. The fold (node and state-sourced indexers alike, through `foldcore.Pass.RequireVerified` and `foldcore.Verifier`) marks an operation whose attestation is absent or does not verify against an attester key in META — decision `unverified`, the history keeping it, the state unchanged.
6. **The SDK routes by the declaration, and reads the mark.** `Apply`, `Create`, `CreateFromSnapshot` and `SaveSnapshot` go through the node when the store requires verified authors or the caller passes `client.Verified()`; otherwise the direct publish stays. Every operation a history read or a tail yields carries `Verified`, checked against the attester keys the client reads from META. The CLI's `instance apply` and `instance create` take `--verified`; `instance history` prints a `VERIFIED` column; `store get` says when a store requires verified authors.
7. **The quick start binds the caller too.** `chronicle up` issues two users: the node's (`node.nk`, unrestricted, the node's signer and the indexers' connection) and the member's (`user.nk`, the member baseline rendered for `admin`). A verb spoken under another's name is refused by the embedded server, as the managed form refuses it.
8. **The artifact, the suite, the words.** `contract/sdk-contract.json` at 3.0.0: the subjects with `<principal>`, no `principal` property, the `apply` interaction, the attestation headers and signed form, the member baseline, `verified_authors` on `store.create` and the store configuration record, `verified` on history and tail items, the `unverified` decision, six new codes. Golden fixture `attestation.json` with the signed form and the verdict over a fixed key; live scenario `03-verified-authors`: a verb under another's name refused, a verified store written through the node, a direct write kept and marked, the node's guard judgement and pre-flight refusal, a record naming another author refused, the opt-in verified write in an ordinary store.

## Constitution check

Against `AGENTS.md` and `how-we-build`:

- **Nothing sits in the direct append path.** Direct publish is untouched; the node is in the path of the writer who asked for a verified author and of nobody else. `AGENTS.md` says so in these words now.
- **The wire contract is the thing under test, against real NATS.** Every new behaviour is proved over the embedded server: the up test, the node tests, the conformance scenario over `up`.
- **The client surface is the artifact's.** The artifact, the Go parity test, the fixtures and the scenarios change in this PR.
- **The words are the user's.** *verified* is the vocabulary's word (chronicle-hq decision 0057); *attested*, *asserted*, *attester* stay in the record and the code's comments.
- **Minimal build.** No signing by the writer, no per-session key registry, no gradual migration of the verb subjects: the beta moves at once, as the decision says.

## What it does not do

No change to `Op-Version`. No store-level setting that can be changed after creation. No refusal on the direct path — a direct write to a store that requires verified authors lands and is marked, never dropped. No attester-key rotation ceremony: the key is the service credential's, and a retired key's record is foreseen in META and unused until a rotation exists. No SDK option to force a direct write into a store that requires verified authors — a bare client can always make that publish, and the suite does so to prove the mark.
