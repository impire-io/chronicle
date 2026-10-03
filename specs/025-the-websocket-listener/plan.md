# Plan — spec 025

1. **`devdir`** — `WebsocketURLFile` (`websocket.url`) and
   `WebsocketURLPath`, beside the client URL's.
2. **`up.Config`** — `WebsocketPort` (0 off, -1 a free port) and
   `WebsocketOrigins`; `Local.WebsocketURL` carries the bound URL.
3. **`startServer`** — with a port, `server.WebsocketOpts` on
   `127.0.0.1`, `NoTLS`, `AllowedOrigins`; the URL from
   `Server.WebsocketURL()`. `Up` writes it to the data dir, or removes a
   stale one when the listener is off.
4. **`Run`** — `--websocket-port`, repeatable `--websocket-origin`; the
   banner prints `ws:` when the listener is on.
5. **Tests** (`up/websocket_test.go`) — the spec's four acceptance reads
   against a real quick start.

Gate: `make check` — fmt, tidy, build, `go test -race ./...`, lint.
