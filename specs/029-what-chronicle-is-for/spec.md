# Spec 029 — what chronicle is for

**Work ID:** `017-what-chronicle-is-for` (chronicle-hq research 017, graduated)
**Design:** `chronicle-hq` @ `f76d8a9` (hq PR #66, merged) —
[`03-DECISIONS/0047-chronicle-is-a-datastore-as-a-service-where-the-writes-are-the-record.md`](../../../chronicle-hq/03-DECISIONS/0047-chronicle-is-a-datastore-as-a-service-where-the-writes-are-the-record.md),
[`00-META/mission.md`](../../../chronicle-hq/00-META/mission.md) § Mission,
[`00-META/audience.md`](../../../chronicle-hq/00-META/audience.md) § what they expect,
and design [`02-DESIGN/13-the-panel.md`](../../../chronicle-hq/02-DESIGN/13-the-panel.md),
whose `lands` block names this repo first after chronicle-hq, independent of
`chronicle-js` and `chronicle-web`.
**Status:** in progress ([plan.md](plan.md)).

## What this delivers

The first screen of this repository says what chronicle is, in the order
decision 0047 fixes: the sentence, the reasons, the when-rule — derived from
`mission.md` and `audience.md`, never written fresh here (0047 § 6). Copy
only; no contract, no wire, no release.

- **The README's opening.** Three parts replace the first paragraph: the
  sentence (*a datastore as a service where the writes are the record*, the
  model in the five words, the indexes as the third clause, the two forms);
  the six reasons as a list — replay · author and name · no lock · live ·
  state derived · a second view is a declaration — the indexes sixth of six;
  the when-rule as one paragraph, whose negative half names a cache, a blob
  and a counter and nothing else — no volume, no query caveat (0047 context).
  The repository paragraph follows them unchanged. Step 4 keeps its heading
  and opens with *when a question is not a list of a type, declare an
  index — a second view, derived from the same history*. The quick start is
  untouched and `cli/quickstart_test.go` still runs it.
- **The root help's first line** (`chronicle --help`) is the sentence's
  one-line form with the model — the same first screen, for the agent that
  reads help instead of a README. `cli/vocabulary_test.go` reads it.

## What it does not do

No change to the CLI's grammar, the client, the contract artifact, the
conformance suite or the wire. No new test: the quick-start test proves the
README still runs, the vocabulary test proves the help still speaks the
user's words; an emphasis guard for the README rides with tracker item
chronicle-38 (0047 § 6).
