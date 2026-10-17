# Spec 037 — a store and a type may be deleted, and a role may be changed

**Work ID:** `chronicle-57` (tracker item; lands after chronicle-hq #99)
**Design:** `chronicle-hq` @ `2854f78` (hq PR #99) — decision
[`03-DECISIONS/0062-a-store-and-a-type-may-be-deleted-and-a-role-may-be-changed.md`](../../../chronicle-hq/03-DECISIONS/0062-a-store-and-a-type-may-be-deleted-and-a-role-may-be-changed.md),
amending [`02-DESIGN/02-wire-contract.md`](../../../chronicle-hq/02-DESIGN/02-wire-contract.md)
§ streams, [`02-DESIGN/03-meta-and-state.md`](../../../chronicle-hq/02-DESIGN/03-meta-and-state.md)
§ store configuration, § type records, § the identity slice,
[`02-DESIGN/04-fleet.md`](../../../chronicle-hq/02-DESIGN/04-fleet.md)
§ the node's duties, [`02-DESIGN/05-indexes.md`](../../../chronicle-hq/02-DESIGN/05-indexes.md)
§ the declaration, [`02-DESIGN/07-the-cli.md`](../../../chronicle-hq/02-DESIGN/07-the-cli.md)
§ the grammar, § types, § members and service accounts, and
[`02-DESIGN/12-the-sdk-contract.md`](../../../chronicle-hq/02-DESIGN/12-the-sdk-contract.md)
§ the conformance suite.
**Status:** implemented on this branch.

## What this delivers

The three sentences the grammar of 0045 implied and the wire did not
answer: `store delete`, `type delete`, `member set-role`. Three verbs on
the account API, admin role, the caller from the subject.

1. **`LOG.DELETE`** (`client.DeleteStore`). Total and irreversible, in a
   fixed order a second run finishes from any step of. The claim is the
   config's status moving to `deleting` by compare-and-set
   (`contract.StoreStatusDeleting`); from it every other verb on the
   store — type define, index declare, a snapshot, the verified write —
   refuses with `store-deleting` (`node.requireStore`, the one check
   they share now), and a `store create` of the name refuses the same
   way. Then: the fold stops; every index declaration but `state` is
   removed and its workload reported retired; the `state` declaration;
   the state bucket; the stream, every operation and schedule with it,
   whatever the history policy — `history: full` binds rollup, purge
   and per-message delete, not the deletion of the stream; the type
   records; the config key last. A node that boots on a store left
   `deleting` finishes the delete instead of starting a fold. The name
   may be created again. A store that does not exist is `no-such-store`.
2. **`TYPE.DELETE`** (`client.DeleteType`). The record goes — an
   undefined type is `no-such-type` — and the store is rebuilt by replay
   under what remains (`rebuildStore`, the path every declaration change
   takes): every instance that resolved through the type, children
   included, is untyped from then on — history whole, no state beyond a
   snapshot's, absent from `instance list` and every state-sourced
   index, a create under it refused as a create of an undefined type —
   `client.Create` now refuses, before anything is sent, a path that
   resolves to no type (`ErrNoType`; `CreateFromSnapshot` stays the
   untyped form), as 0021 § 2 always required of pre-flight. Defining
   the type again restores all of it on the re-fold.
3. **`MEMBER.SET_ROLE`** (`client.SetRole`). One KV update of the
   membership's `role` under the entry's revision, nothing else touched
   — a member's or a service account's alike; the credential keeps
   connecting and the next request is judged by the new role. The role
   already held is answered. **The sole admin keeps the role**: a change
   that would leave the account without an admin is `sole-admin`, and
   `MEMBER.REVOKE` takes the same guard (`node.soleAdmin`). Under any
   resolver but the registry the verb is refused with
   `membership-elsewhere`, like add and revoke (0061).
4. **The contract.** Three interactions in the artifact — `store.delete`,
   `type.delete`, `member.set-role` — and three codes in the catalog;
   `store-deleting` joins every interaction that acts in a store,
   `sole-admin` joins `member.revoke`. The version bumps minor, 3.3.0.
   The conformance suite gains the scenario `05-deleting-and-roles`
   and the runner the sentences (`store.delete`, `type.delete`,
   `member.add`, `member.set-role`, `member.revoke`) and a `roles`
   expectation on `list.members`.
5. **The CLI.** `store delete NAME` names what goes and acts, with no
   `--force` and no prompt; it clears the selection when the store was
   the selected one. `store list` gains a STATUS column and `store get`
   says *deleting*. `type delete TYPE` says how many instances become
   untyped — read from the state, top-level instances of the type —
   then acts. `member set-role LOGIN ROLE` and `service-account
   set-role NAME ROLE` speak the one verb for the two nouns. The
   workaround `set-role` taught is gone.

## Tests

- `node`: `TestStoreDelete` — nothing of the store remains (stream, state
  bucket, every META key), a writer is refused, a missing store is
  refused by name, the name is created again empty, a `history: full`
  store deletes; `TestStoreDeleteFinishesFromAnyStep` — the claim alone
  refuses every verb and a create of the name, a second delete finishes,
  a node booting on a store left deleting with its stream already gone
  finishes it; `TestTypeDelete` — the instances leave the listing with
  their history whole, a sibling type is untouched, a create of the
  deleted type is refused, defining it again brings the state back;
  `TestSetRole` — the next request is judged by the new role, the record
  keeps its key and kind, a service account changes the same way, the
  sole admin keeps the role under set-role and revoke, the gates.
- `client`: the artifact test proves the three interactions' subjects,
  shapes, request and reply schemas against the Go types, and the codes
  against the catalog.
- `cli`: the spine runs `set-role` on both nouns, refuses demoting the
  only admin, deletes a type and a store and sees the lists follow; the
  vocabulary test reads the new help.
- `conformance`: scenario 05, over `chronicle up`.

## What it does not do

No soft delete, grace period or export before a store delete. No
refusal of a type delete while instances exist: the fold already defines
an instance of an undefined type, and the delete makes the definition
apply. No config edit beyond the status. No identity's own account
destroy. No change to the managed service, the TypeScript SDK or the
console here: `chronicle-js` and `chronicle-web` follow in the `lands`
order on chronicle-57. No release: the version is cut by hand after this
lands.
