# Plan 016 — emit the cluster's server configs

## Layout

```
internal/mint/clusterconfig.go   ClusterConfig{Nodes, ClientPort, ClusterPort,
                                 ClusterName, TLSCert, TLSKey, StoreDir,
                                 ResolverDir} + (b *Bootstrap) EmitClusterConfigs
                                 → []NodeConfig{Name, Content}; validation
                                 (≥1 node, unique non-empty names/hosts,
                                 cert+key both or neither); preload iterates
                                 the bootstrap's accounts generically
internal/mint/clusterconfig_test.go
                                 unit: validation refusals, rendering
                                 (routes complete, preload complete, tls
                                 present/absent);
                                 integration: emit 3 loopback configs →
                                 ProcessConfigFile → boot 3 real servers →
                                 mint an account through the JWT driver
                                 against node 1 → its user connects to
                                 nodes 2 and 3 (the design's verified
                                 mechanism, now a regression test)
internal/fleet/fleet.go          EmitClusterConfig(args, out): flags, writes
                                 <out>/<name>.conf 0644, prints the copy-to-
                                 host instruction per node
cmd/chronicle/main.go            case "operator emit-cluster-config"
internal/cli/cli.go              usage line beside rotate-signing-key
```

## Order

1. `clusterconfig.go` rendering + validation, unit tests green.
2. Integration test (guarded by the same embedded-server pattern
   `mint_test.go` already uses; loopback ports picked free).
3. `fleet` wrapper + `main.go` dispatch + usage line.
4. `make check`; ready.
