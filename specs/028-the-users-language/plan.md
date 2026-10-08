# Plan 028 — the user's language

## Layout

```
contract/names.go         UpperStore, StoreSubjects, InstanceFromSubject; PathTail, TailPath
contract/logname.go       NamePattern, InstanceTokenPattern, ValidateStoreName, ValidateInstance, ValidatePath
contract/metakeys.go      MetaStore*, StoreConfig, HistoryFull, TypeRecord.Children, Membership.Kind
contract/source.go        SourceHistory
contract/resolve.go       ResolveInstance — the children maps
contract/errors.go        the renamed codes, meanings in the user's words
contract/sdk.go           ContractVersion 2.0.0
contract/sdk-contract.json  the artifact, regenerated field by field
client/api.go             StoreCreate*, InstanceSnapshot*, Children, Instance fields; paths in, paths out
client/ops.go             Create, CreateFromSnapshot, Apply, SaveSnapshot; ErrInstanceExists, ErrInstanceMoved, ErrUndeclaredChild
client/iter.go            ListStores, ListInstances with ByType / Under / Where; History
client/read.go            State, GetType, GetIndexDeclaration, Resolve on paths
node/*                    the handlers under the new request shapes; reasons in the user's words
index/*                   SourceHistory; hits and visits answer paths
foldcore/*                ResolveInstance, StoreFingerprint
cli/                      rewritten: cli.go (dispatch, help, flags, output), context.go, store.go,
                          types.go, instances.go, indexes.go, members.go, login.go, output.go
cmd/chronicle/main.go     `up` saves and selects the local context
conformance/              fixtures and scenarios in the new words; the two new list scenarios
cli/vocabulary_test.go    no retired word on any surface
cli/quickstart_test.go    the README's quick start, executed
README.md, AGENTS.md      the new sentences; the vocabulary line
```

## Mechanics

- **One rename, then the grammar.** The Go identifiers are renamed
  mechanically across every package first (whole-word), the build is
  made green, and only then is the CLI rewritten — so the client's
  surface is settled before the adapter restates it.
- **Paths at the edge.** The client converts a path to its dotted tail
  before it touches a subject or a key, and converts tails back to paths
  in everything it yields; the services answer paths in hits by the same
  function; subjects, keys and the fold never see a slash.
- **Listing is a value scan.** `ListInstances` watches the state bucket
  with values (not keys-only) when a filter needs them; `ByType` and
  `Under` are key-shape tests (token count and prefix), `Where` is a
  scalar equality on the folded state. The CLI's `--sort` sorts what the
  scan yielded; a list is small by the time a human reads it, and the
  message says an index is the answer past that.
- **Help is data.** Each command carries its usage line and summary; the
  root help lists nouns, a noun's help lists verbs, a verb's help prints
  its usage and flags. `-h`/`--help` anywhere in the arguments prints and
  exits 0. Connection flags are registered on every connecting verb but
  hidden from its help, which points at `context --help`.
- **Tables** are `text/tabwriter` with a header row; `--output` switches
  the whole command's output; `yaml` goes through `sigs.k8s.io/yaml` so a
  YAML type file is JSON Schema underneath.
- **The quick start test** reads `README.md`, takes the fenced block after
  the heading that says *run the quick start*, boots `up` in-process in a
  temp dir, and executes each `chronicle …` line through `cli.Run` with a
  minimal shell: `>` redirects stdout to a file in the temp dir, `#…` is
  dropped, the `chronicle up` line is the boot.
- **The retired-word test** renders every help text and runs the sentences
  of the quick start, then greps the output for the retired list.
