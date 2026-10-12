# Spec 032 — the install profile lists the solutions the environment hosts

**Work ID:** `chronicle-67` (tracker item; lands after chronicle-hq #80,
before chronicle-js, chronicle-service, chronicle-web and chronicle-ops)
**Design:** `chronicle-hq` @ `88fa386` (hq PR #80) — decision
[`03-DECISIONS/0054-a-solution-is-enabled-from-the-console-by-its-install-manifest.md`](../../../chronicle-hq/03-DECISIONS/0054-a-solution-is-enabled-from-the-console-by-its-install-manifest.md)
point 2, amending [`02-DESIGN/08-browser-identity.md`](../../../chronicle-hq/02-DESIGN/08-browser-identity.md)
(the profile's `solutions`) and [`02-DESIGN/14-solutions.md`](../../../chronicle-hq/02-DESIGN/14-solutions.md)
§ the install manifest (the catalogue).
**Status:** implemented on this branch.

## What this delivers

Decision 0054 makes enabling a solution a button in the console, driven by
an install manifest the solution publishes — and makes the environment's
install profile the catalogue of what may be enabled: the solutions it
hosts, each with the address of its manifest. The profile's shape is the
open contract's (decision 0043: the client half of the bridge), so the
field lands here first and the service, the SDK and the console follow.

1. **`bridge.Profile` gains `Solutions`** — `solutions` on the wire,
   omitted when empty — a list of `bridge.Solution`: the solution's
   `name` and its `manifest`, the address of the document that says what
   enabling it means (chronicle-hq design 14 § the install manifest).
2. **Nothing else here reads it.** `DecodeProfile` accepts a profile
   without the field, as every install published before this; `login`
   ignores it. Control (chronicle-service) writes it from the flag the
   environment sets; the console (chronicle-web, on chronicle-js) reads it.

## Tests

- `bridge`: a profile without `solutions` decodes to none; one with it
  keeps each name and manifest address; both round-trip, and the key is
  absent from the encoding when the environment hosts none.

## What it does not do

No change to the SDK contract artifact or the conformance suite: the
profile stays outside `sdk-contract.json` (decision 0043 point 4). No
reading of the catalogue in this module — no CLI sentence lists or enables
a solution; that is the console's. No manifest here: the shape is design
14's and the document is each solution's own. No release: the version is
cut by hand after this lands.
