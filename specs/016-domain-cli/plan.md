# Plan 016 — the CLI speaks the domain

## Layout

```
internal/cli/context.go   the context store: chronicle's own records under
                          os.UserConfigDir()/chronicle (CHRONICLE_CONFIG_HOME
                          overrides the root) — contexts/<name>.json holding
                          {url, creds, log}, a `current` file naming the
                          selection; save/select/list/show/rm and the
                          field-wise resolution (flags > CHRONICLE_CONTEXT /
                          CHRONICLE_LOG > the selected context > the --dir
                          fleet url fallback), with the teaching errors for
                          a missing log or creds
internal/cli/cli.go       dispatch rework: the sentences (create/do/get/
                          history/rollup), the nouns (context, log, type,
                          operation|op, index), query, things, sectioned
                          usage; append/state/replay/thing/semantic/graph
                          retired from dispatch and usage
internal/cli/inspect.go   readable `type inspect` and the `type define`
                          echo-back; the `type init` skeleton
client/read.go            ListLogs, ListIndexes, GetIndexDeclaration,
                          ListThings (state keys, the fold watermark
                          excluded), Resolve (the tail walk, exposed)
client/ops.go             CreateWith: birth through a declared operation —
                          pre-flight, publish with the create-if-absent
                          guard, the same landed/exists arbitration as
                          CreateThing
README.md                 quick start rewritten to the new sentences
```

## Mechanics

- **The context store is CLI-layer, not SDK.** The client keeps taking
  `(url, creds)`; resolution lives beside the flag parsing. Control-plane
  verbs (`up`, `operator`, `tenant`, `member`) keep `--dir` and never read
  a context; `tenant create` and `member add` write and select one after
  minting, so the next sentence works.
- **`create` resolves before it publishes.** The exposed `Resolve` names
  the tail's kind: typed → `CreateWith` on the `create` operation (or
  `--op`), whose payload pre-flights against the operation's schema;
  untyped → `CreateThing` with the payload as birth state; undeclared →
  the SDK's aspect refusal passes through. `ErrUndefinedOperation` on the
  constructor path is wrapped with the type's operation list — the
  teaching error.
- **`CreateWith` mirrors `CreateThing`, not `Append`.** Same
  create-if-absent guard (expected sequence 0), same guard-refusal
  arbitration (own op landed → success; someone else → `ErrThingExists`);
  the payload rides the declared operation instead of the snapshot
  envelope. Birth refuses `WithExpectedSeq` — it guards at 0 by
  definition.
- **The operation verbs are read-modify-write.** `GetType` → mutate the
  operations facet → `DefineType` whole; the node bumps the revision as
  it does for any redefinition (0021). Two racing vocabulary edits
  resolve by latest-declaration-wins — the race is an admin racing
  themselves and is accepted, not locked. Before the write, the CLI
  prints what it is about to cause: an effect change names the rebuild;
  `rm` names the none-with-warning re-fold of the operation's history.
- **`query` reads the declaration first.** `GetIndexDeclaration` names
  the kind: search/semantic take the joined text; graph requires
  `--from`, `--depth` absent → neighbors (with `--direction`, `--label`),
  present → walk (with `--direction`, `--labels`); state declines to
  `get`; bare invocations print the kind's accepted arguments instead of
  erroring blind.
- **Discovery reads what is at rest.** Logs from `log.<log>.config` META
  keys, indexes from `index.<log>.*` declarations, things from
  `STATE_<LOG>` keys with `=fold` excluded — no new verbs, no scans the
  design forbids.
- **Tests ride the existing spine.** `cli_test.go` reworks onto the new
  sentences with `CHRONICLE_CONFIG_HOME` pointed at a temp dir per test;
  the spine test runs the design's day-in-the-life verbatim, then the
  refusals (no selection, constructor missing, stale guard, untyped
  rollup veto, undeclared aspect). Client list/CreateWith paths get wire
  tests against the embedded fleet beside the existing ops tests.

## Order

1. `client/read.go` + `client/ops.go` additions, wire-tested.
2. The context store with its own unit tests (no NATS needed).
3. The dispatch rework and usage, spine test rewritten alongside.
4. `type init`/echo-back/readable inspect, operation verbs.
5. `query` unification; retire the old verbs from dispatch last so the
   spine test flips in one move.
6. README quick start; `make check` green with race.
