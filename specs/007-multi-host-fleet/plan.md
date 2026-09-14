# Plan 007 — the multi-host fleet

1. **Contract**: the `CHRONX` root, `FleetBridgeLocalSubject`
   (`CHRONX.FLEET.REPORT`), the stamped export pattern and per-tenant
   form, and the `FleetIndexReport` payload (`action` declared|deleted,
   log, index, kind).
2. **Mint**: the control account's service export (init + the existing
   load-time refresh path); `JWTDriver.ControlAccountPub` and the
   per-tenant import at `MintAccount`.
3. **Workloads**: the bridge endpoint on the stamped wildcard — tenant
   from the subject token, dispatch/stop reuse of the handler logic.
4. **Node**: the default bridge reporter from the node's own conn;
   soft-fail reports and boot re-derivation everywhere.
5. **Fleet + cmd**: delete `metareporter.go` and the composition's
   `indexReporter`; drop `InProcess.NodeConfig` wiring to the default;
   `cmd/chronicle-executor` standalone main.
6. **Wire tests** on the operator-mode server: stamped report →
   dispatch; member refused at the wire; suite green; gate; live
   two-executor read (embedded inprocess + standalone microsandbox on
   one machine — the first heterogeneous fleet).
