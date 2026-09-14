# Plan 006 — the microsandbox backend

1. **`internal/guestnet`**: `ResolveURL(url)` — swap the literal
   `msb-gateway` host for the default gateway parsed from
   `/proc/net/route`; a seam for the route source so the parser is
   unit-tested with captured tables.
2. **`cmd/chronicle-workload`**: the scheduled entrypoint — kind switch
   over `node.Start` / `search.Start`, `guestnet.ResolveURL` on the URL,
   `client.ConnectFile` for the creds. `make workload-linux` builds it
   static for linux/arm64.
3. **`internal/executor/msb.go`**: the `Microsandbox` backend behind the
   existing `Backend` seam — config {Image, WorkloadBinary, HostURL,
   MSBPath}; `Start` = staged creds file + `msb run` child process;
   `Done` = process exit; `Stop` = kill + `msb rm`. Command construction
   extracted pure and unit-tested; a best-effort `msb rm` precedes every
   start so a crashed predecessor's name never blocks a placement.
4. **Composition**: `fleet.Config.Backend`/`WorkloadBinary`,
   `chronicle up --backend --workload-binary`, executor wiring unchanged.
5. **Gate + live slice**: `make check` green with no msb dependency in
   any test; then the maturity verification of the spec, live, recorded
   on the item.
