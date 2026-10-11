# Plan 029 — what chronicle is for

## Layout

```
README.md        the opening: the sentence, the reasons, the when-rule; step 4's first sentence
cli/cli.go       rootHelp: the first line
```

## Mechanics

1. The README's first paragraph becomes three — the sentence, the reasons as
   a list, the when-rule — in the words of `mission.md` § Mission and
   `audience.md` § what they expect at hq `f76d8a9`, line-wrapped as the file
   is. The paragraph on the open account plane, *Getting started* and
   everything below stay as they are; step 4's first sentence changes.
2. `rootHelp` prints the sentence's one-line form and the model on its first
   line; every other line stands.
3. `make check` — fmt, tidy, build, test, lint — green: `TestHelpSpeaksTheUsersLanguage`
   reads the new line, the quick-start test runs the README's block.
4. One PR, draft from the first push, labelled `work/017-what-chronicle-is-for`;
   ready and merged after chronicle-hq #66 (already merged), independent of
   `chronicle-js` and `chronicle-web`. No release: the README is read on
   GitHub and the help line ships with the next tag whatever it is.
