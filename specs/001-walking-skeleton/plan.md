# Plan 001 — the walking skeleton

## Layout

Mirrors the `hits` repo's proven shape: `contract/` is the one shared leaf,
`client/` speaks only contract, services stand alone, a composition root
carries `chronicle up`, and depguard enforces every boundary.

```
contract/                  the wire contract as code, declared once:
  names.go                   LOG_/META/STATE_ names, the <LOG> mapping,
                             subject grammar, API verb subjects
  record.go                  the Op-* header set; op record build/parse
  logname.go                 log-name rule [a-z0-9-]+, reserved list
  streams.go                 LOG_<LOG> stream settings (0008 table),
                             META/STATE bucket settings
  metakeys.go                META key grammar: log.<log>.config,
                             log.<log>.type.<op.type>, identity.*
internal/mint/             the driver seam and the jwt driver:
  driver.go                  Driver interface — mint account, issue user
  jwt.go                     operator-mode minting: account JWT with
                             JetStream limits, signing key + member-
                             baseline scoped key, claims push, verify
                             by connecting
  bootstrap.go               the throwaway operator: generated operator,
                             system account, full (dir) resolver — used
                             by `chronicle up` and the tests alike
internal/control/          chronicle-control micro service: TENANT.MINT
                             (provision META, seed identity registry,
                             first admin principal, .creds back)
internal/node/             chronicle-node: LOG.CREATE and SCHEMA.SET
                             verbs, the per-log folds, STATE_<LOG>
                             maintenance under revision CAS
internal/fleet/            `chronicle up` composition: bootstrap NATS +
                             control + a node per minted tenant, one
                             process, no scheduler
internal/cli/              the chronicle CLI verbs over client/
internal/natstest/         embedded servers for tests: plain JetStream
                             and full operator-mode (trusted operator,
                             dir resolver, system account)
internal/version/
client/                    the Go client: Connect, CreateLog, SetSchema,
                             CreateThing (birth), Append (pre-flight),
                             State, Replay, Fold
cmd/chronicle/             CLI: up, tenant create, log create, append,
                             state, replay
cmd/chronicle-control/     standalone control main
cmd/chronicle-node/        standalone per-tenant node main
```

## Mechanics

- **API verb subjects.** The designs fix only the `CHRON.API.>` root;
  the concrete verbs are protocol tokens, so uppercase per the case
  rule: `CHRON.API.LOG.CREATE`, `CHRON.API.SCHEMA.SET` on the node,
  in-account. Control's cross-account surface is *not* tenant wire
  contract; it serves `CHRON.CTRL.TENANT.MINT` inside the control
  account. Requests carry the caller's principal ID; the node checks the
  registry role (`admin` for both verbs). That assertion is exactly as
  strong as `Op-Author` — the accepted trust tier (items 23/24).
- **The member baseline** (scoped signing key template): allow pub
  `CHRON.>` plus the read-side JetStream API — `$JS.API.CONSUMER.>`,
  `$JS.API.STREAM.INFO.>`, `$JS.API.STREAM.NAMES`,
  `$JS.API.STREAM.MSG.GET.>`, `$JS.API.DIRECT.GET.>` — and allow sub
  `CHRON.>`, `_INBOX.>`. A member can append, replay, and read state;
  a member cannot create or delete streams, purge, or write KV — the
  0003 ownership boundary held at the wire. The tenant service user is
  issued under the account signing key, unscoped. Tests assert both
  sides (a member's stream delete is refused; the node's isn't).
- **The generic fold and the state bucket.** The designs pin the value
  (`{seq, state}`) and the writer (the node), but chronicle cannot apply
  arbitrary customer op semantics — hq defines no default op-application
  rule, and inventing one here would mint contract by code. The skeleton
  folds to the pattern's own floor: on every **snapshot** op the node
  CAS-writes `{seq: <snapshot's stream seq>, state: <snapshot state>}`;
  non-snapshot ops advance nothing in the bucket. An exact reader does
  precisely what the design prescribes — read the value, fold the log
  from `seq + 1` — with its own semantics via the client's `Fold`
  helper. Birth is a snapshot, so state exists from a thing's first
  message. The gap (should chronicle define a default op-application
  semantic?) is filed as tracker item 27 rather than settled here.
- **Fold hygiene.** One ordered consumer per log on
  `CHRON.<log>.OPS.>`, one fold state per subject. Unknown op types:
  warn and continue. Known type, schema-invalid payload: mark — a
  structured warning naming log, subject, and op ID — never drop. The
  value shape stays exactly `{seq, state}`.
- **Birth and appends** are the client's, end to end: `Nats-Msg-Id`
  from nuid kept across retries, `Op-Author` from the creds JWT name,
  one in-flight publish per subject, pre-flight validation against
  `log.<log>.type.<op.type>` schemas (santhosh-tekuri/jsonschema),
  birth publishing the first snapshot with
  `Nats-Expected-Last-Subject-Sequence: 0`. Guards are never proxied:
  dedup and CAS failures surface as the server's own errors.
- **Minting** (the `jwt` driver, the skeleton's only driver): account
  nkey; account JWT signed by the operator signing key with JetStream
  enabled; an account signing key and the member-baseline scoped key;
  push to `$SYS.REQ.CLAIMS.UPDATE` over the system-user connection;
  **verify by connecting** before reporting success. Users are minted
  offline: service user under the account key, members under the scoped
  key (permission-empty, name = principal ID). Control then provisions
  `META` and seeds the identity slice (principal + membership, role
  `admin`) as the service user, and returns the first admin's `.creds`.
- **`chronicle up`**: bootstrap material (operator, system account,
  resolver dir, control account) generated into a data dir on first
  run and reused after; embedded nats-server in operator mode; control
  and a node-per-tenant in-process — control tells the composition
  root about each mint, the root starts that tenant's node on the
  tenant's service creds. `~/.chronicle/dev` by default, `--dir` to
  move it; `control.creds` and the client URL land there for the CLI.
- **Dependencies**: nats-server v2 (embedded), nats.go, jwt/v2, nkeys,
  nuid, santhosh-tekuri/jsonschema/v6. No natscontext yet — skeleton
  connections are URL + creds; contexts arrive with a real operator
  flow.

## Tests

Contract: pure tables — log-name acceptance and the reserved list, the
`<LOG>` mapping, subject build/parse round-trips, op record header
round-trips, stream settings against the 0008 table. Mint (operator-mode
embedded server): mint → verify-by-connect; member baseline enforced
positively (append, replay, state read) and negatively (stream delete,
KV put, foreign-subject pub all refused); dedup and birth-CAS behavior
server-enforced through a member connection. Node+client (per-tenant,
embedded): create log → stream exists with the 0008 settings; reserved
and malformed names refused; schema set → pre-flight refuses an invalid
payload before the wire, valid ops land; fold writes `{seq, state}` on
birth and on a later snapshot; unknown types warn, invalid payloads
mark, neither drops; State + exact Fold read; Replay returns the full
tail in stream order. Fleet: `up` in a temp dir → mint tenant → the
whole FR-01→FR-05 spine as one test, the same flow the CLI drives.
CLI: verbs round-trip against an `up` fleet; bad flags fail before
dialing. Everything runs `-race` against real embedded servers; no
mocked NATS anywhere.
