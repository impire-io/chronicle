# Plan 034 — a parent's children in order, with their types; a store's size

## Layout

```
client/paths.go    ListInstances resolves the parent on every listing under
                   it; childTypeOf takes the parent's children map and a
                   segment name and answers the declared type, else the name
client/read.go     StoreUsage {bytes, max_bytes} and Client.StoreUsage, a
                   stream info read beside GetStore
cli/instances.go   --sort defaults to seq under --in; the help says so
cli/store.go       store get prints the size line from StoreUsage and
                   carries bytes and max_bytes in its JSON; describeBudget
                   names the default
cli/cli_test.go    the spine: a second child created later lists first under
                   --in, the TYPE column reads comment, jsonl carries
                   "type":"comment"; store get shows 0 of the default budget
```

## Mechanics

- **One resolve per listing.** Resolving the parent is a META read the
  listing under a parent already paid when `--type` narrowed it; paying
  it on every `--in` is the price of naming each child's type, and a
  listing is small by the time a human reads it (decision 0045).
- **The budget in effect is the stream's.** `StoreConfig.MaxBytes` is
  the store's own override, zero for the default; the stream's
  `Config.MaxBytes` is what the node set, the account cap applied. The
  CLI prints the stream's number and names the default from the
  setting.
- **No version bump.** The artifact's `list.instances` item already
  says what `type` is; `StoreUsage` joins the single reads the artifact
  does not enumerate. `ContractVersion` stands.

## Tests

`TestCLISpine` (real embedded NATS, race on): after `comments/c-1`, a
`comments/c-0` created later lists first under `--in invoice/inv-1`; no
row carries `comments` as its type and the jsonl form carries
`"type":"comment"`; `store get orders` prints `size: 0 of 1073741824
bytes used (the default budget)`.
