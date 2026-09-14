# Spec 006 — the microsandbox backend

**Work ID:** `chronicle-2` (tracker item chronicle-2)
**Design:** `chronicle-hq` @ `1738709` —
[`02-DESIGN/06-scheduler.md`](../../../chronicle-hq/02-DESIGN/06-scheduler.md)
(§ the executors, § backends: microsandbox first, § the workload contract)
and [`03-DECISIONS/0014-the-fleet-runs-on-chronicle.md`](../../../chronicle-hq/03-DECISIONS/0014-the-fleet-runs-on-chronicle.md)
(part 7: microsandbox pinned pre-1.0, the walking-skeleton slice as its
maturity verification).
**Status:** implemented on this branch ([plan.md](plan.md)) — awaiting
review and merge. The maturity verification ran live on the dev host
(msb 0.6.8, macOS/HVF): mint booted `chron-acme-node` as a microVM (creds
pulled record-verified, host NATS reached through the per-sandbox
gateway), the declared index brought `chron-acme-index-orders-text` up
beside it and the query answered `invoice-1 0.2170` from inside the
guest, `msb stop` on the node's sandbox was restarted by the executor's
budget within a second (attempt 1), and deleting the declaration retired
the indexer's microVM. One teardown nit observed and accepted: a
SIGTERM'd composition can leave the last sandbox's stopped record behind
— the pre-start `msb rm` makes it harmless to every successor.

## What this delivers

The first 0004 backend: placements as microVMs. The loop of spec 005 is
untouched — the same auction, the same guard, the same creds pull — with
`ensure/status/destroy` actuated by `msb` instead of goroutines. Probed
against msb **0.6.8** on the dev host (macOS/HVF): `--net host` gives the
guest a per-sandbox gateway that proxies to the host's loopback (verified
by fetching a 127.0.0.1-bound listener from inside a sandbox), and
`--copy-file` stages files into the guest rootfs before boot.

- **`cmd/chronicle-workload`**: the one binary a scheduled placement runs
  — `--kind node|index-search --log --index --url --creds`. It is the
  scheduled form of the fleet's workload kinds; `chronicle-node` stays the
  operator's standalone form. Built static (`CGO_ENABLED=0`) for the
  guest's linux/arm64 by the `make workload-linux` target.
- **Guest-side gateway resolution** (`internal/guestnet`): the sandbox's
  NATS URL cannot be known before boot — each sandbox gets its own /30 and
  gateway — so the URL the backend passes uses the literal host
  **`msb-gateway`**, and the workload binary resolves it from the guest's
  routing table (`/proc/net/route`). Unit-tested against captured route
  tables; refused off Linux.
- **The `Microsandbox` backend** (`internal/executor/msb.go`), behind the
  same `Backend` seam as backend zero: `Start` writes the placement's
  creds to a private temp file, launches
  `msb run <image> --name chron-<tenant>-<workload> --net host
  --copy-file <binary>:/chronicle-workload --copy-file <creds>:/…`
  with the workload command, deletes the host-side creds file the moment
  the copy is staged, and supervises the foreground `msb run` process:
  its exit is the placement's death (the executor's local restart budget
  applies unchanged), stop kills it and `msb rm`s the sandbox. Command
  construction is unit-tested; the substrate itself is validated by the
  live slice, not by CI (below).
- **Backend selection in the composition**: `chronicle up
  --backend inprocess|microsandbox [--workload-binary PATH]` — the
  operator configures the host's one backend, exactly the self-hosted
  shape the design names. The default stays `inprocess`; `microsandbox`
  requires the workload binary path.
- **The image**: a cached base image (default `alpine`) with the workload
  binary copied in at boot. No published chronicle image exists because no
  release exists ([how-we-deploy](../../../chronicle-hq/00-META/how-we-deploy.md)
  — nothing is built, nothing is deployed); the embedded-binary image is
  release engineering and arrives with the first release flow. The
  backend takes the image name as configuration, so that arrival changes
  a default, not a contract.

## The maturity verification

0014 pins microsandbox pre-1.0 and names the walking-skeleton slice as
its verification, run live on the dev host: `chronicle up --backend
microsandbox` → mint → the node comes up as a microVM (creds pulled
record-verified, mounted into the guest, NATS reached through the
gateway) → create log, thing → declare a search index → the indexer
microVM joins and the query serves → kill the node's sandbox and watch
the executor's budget restart it → delete the declaration and watch the
sandbox retire. The msb surface this build uses — `run --name --net
--copy-file --entrypoint-args`, `stop`, `rm` — is the pinned API; an msb
upgrade is a deliberate revisit of exactly that list.

## CI, stated honestly

The wire tests stay on backend zero and embedded NATS — green anywhere.
The msb-touching pieces are unit-tested to the substrate's edge (command
construction, gateway resolution) and live-verified where msb exists;
no test skips, no test requires msb. KVM-in-CI (or macOS runners with
HVF) remains open until a CI environment exists to check against —
carried on the item, not silently dropped.

## Out of scope

Docker and kubernetes backends (their 0014 triggers have not fired).
Snapshot/restore (scale-to-zero's door stays closed until the bill is
real). A published OCI image and registry flow (waits for the release
flow). Detached sandboxes surviving executor restarts (the foreground
`msb run` ties placement lifetime to the executor process — the same
property backend zero has; the detached form is an improvement the
multi-host increment can take up). The tenant-stamped bridge and
per-component control-plane users (multi-host, unchanged from spec 005).
