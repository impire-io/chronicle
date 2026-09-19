# Spec 018 — emit the cluster's server configs

**Work ID:** `07-hosted-environment` (chronicle-hq design 09)
**Design:** `chronicle-hq` @ `7306847` —
[`02-DESIGN/09-hosted-environment.md`](../../../chronicle-hq/02-DESIGN/09-hosted-environment.md)
§ the substrate, § the stand-up ceremony, per decision
[`0027`](../../../chronicle-hq/03-DECISIONS/0027-the-hosted-environment-runs-on-our-own-cluster-at-scaleway.md).
**Status:** in progress on this branch ([plan.md](plan.md)).
**Superseded in part by spec [020](../020-custody/spec.md)** (decision
0031): rendering node configs is the environment's, so `chronicle
operator emit-cluster-config` left the CLI; the renderer this spec built
stays in `internal/mint` as the substrate the trio test boots from, with
`jetstream { cipher, key }` per node as design 10 asked.

## What this delivers

The hosted environment's one build gap: turning a bootstrap dir into the
`nats-server` configs a real cluster runs. Today the bootstrap's server
half exists only embedded (`Bootstrap.ServerOptions`, single node,
loopback); the design's stand-up ceremony needs the same operator
material serving a clustered, TLS-fronted fleet of external
`nats-server` processes.

```
chronicle operator emit-cluster-config --dir D --out CONF_DIR \
  --node <name>=<host> [--node ...] \
  [--client-port 4222] [--cluster-port 6222] [--cluster-name CHRONICLE] \
  [--tls-cert PATH --tls-key PATH] \
  [--store-dir /var/lib/chronicle/jetstream] [--resolver-dir /var/lib/chronicle/resolver]
```

One self-contained `<out>/<name>.conf` per `--node`, carrying:

- `server_name`, client listen on the node's host, the shared cluster
  name, cluster listen, and routes to **every** node's cluster address
  (self included — nats-server tolerates it, and identical route lists
  keep the configs order-independent);
- the **operator JWT inline**, `system_account` by public key;
- a **full dir resolver** at `--resolver-dir` with `resolver_preload`
  carrying **every account the bootstrap holds** (SYS and CONTROL
  today; whatever joins the bootstrap later — spec 017's AUTH among
  them — rides along without touching this ceremony);
- JetStream at `--store-dir`;
- a `tls` block when `--tls-cert`/`--tls-key` are given (paths are
  target-host paths, emitted verbatim, never read locally) — both or
  neither, refused otherwise.

Like `up` and `rotate-signing-key`, this is the composition root's
ceremony against the fleet dir's custody, not a product-surface verb:
it dispatches from `cmd/chronicle` beside them, wrapped in
`internal/fleet`, implemented in `internal/mint`.

## Contract

- **The emit is pure rendering.** It loads the bootstrap (generating one
  on first run exactly as `LoadOrInitBootstrap` always has), touches no
  network, and writes only under `--out`. Custody stays in the dir; the
  configs carry no seeds — the operator JWT and account JWTs are public
  material.
- **At least one node; names and hosts non-empty; names unique.**
  Duplicate names or a malformed `--node` refuse with the flag named.
  One node is legal — the beta tier of design 09 is this ceremony with
  a single `--node`.
- **Verified the way onboarding demands**: the test boots real servers
  from the emitted files (`server.ProcessConfigFile`, the embedded
  server — never a mocked client), pushes a freshly minted account to
  one node, and proves its user connects to the others. Could not
  succeed if the rendering were broken.

## Out of scope

- The websocket listener — it joins the config when the panel does
  (design 09 names the moment).
- systemd units, host provisioning, DNS, cert issuance — runbook
  material, not rendering.
- The AUTH account itself — spec 017 (design 08) mints it; this emit
  already preloads whatever accounts exist.
- Any change to `Bootstrap.ServerOptions` or `up` — the embedded shape
  stays the dev shape.
