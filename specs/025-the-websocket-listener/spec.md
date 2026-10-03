# Spec 025 — the websocket listener on `chronicle up`

**Work ID:** `chronicle-41` (tracker item; umbrella chronicle-40)
**Design:** `chronicle-hq` @ `6966fa8` (hq PR #42) —
[`02-DESIGN/09-hosted-environment.md`](../../../chronicle-hq/02-DESIGN/09-hosted-environment.md)
§ the websocket listener, for
[`02-DESIGN/13-the-panel.md`](../../../chronicle-hq/02-DESIGN/13-the-panel.md)
(decision 0037).
**Status:** in progress on this branch ([plan.md](plan.md)).

## What this delivers

A browser speaks NATS only over a websocket, and the quick start's
embedded server has no websocket listener — so a console running on the
developer's machine has nothing to dial. `chronicle up` gains one, behind
a flag:

```
chronicle up [--websocket-port N] [--websocket-origin ORIGIN ...]
```

- **Off by default.** Without `--websocket-port` nothing listens beyond
  the client port, exactly as today.
- **Loopback, in the clear.** The listener binds `127.0.0.1` with no TLS —
  the one place design 09 allows a clear websocket: nothing but the
  machine itself can reach it. `-1` picks a free port.
- **Origins.** `--websocket-origin` (repeatable) names the origins a
  browser may connect from (`http://localhost:3000`); the server refuses a
  handshake from any other origin. With none named, any origin may open
  the socket — the quick start is a development convenience on loopback,
  and the connection still has to authenticate.
- **Authentication is unchanged.** A websocket connection is the same
  user on the same one account: the quick start's nkey, nothing else.
- **Recorded beside the client URL.** The running server writes its
  websocket URL to `websocket.url` in the data dir, and `up` prints it; a
  boot without the listener removes a stale one, so the file never names
  a socket that is not there.

## Acceptance

Against a real `chronicle up` (no mocked NATS):

1. With `--websocket-port -1`, the recorded `websocket.url` is the
   server's, and the admin connecting over it with the user's nkey
   creates a log and a thing and reads the thing's folded state.
2. A handshake carrying an origin outside `--websocket-origin` is refused;
   one carrying a named origin is placed.
3. An anonymous websocket connection is refused, as on the client port.
4. Without the flag, no `websocket.url` exists — including after a boot
   with the listener in the same dir.

## Out of scope

The managed dev fleet's listener and the bridge over websocket
(`chronicle-service`, same work ID); the hosted listener behind the
service host's proxy (`chronicle-ops`, chronicle-42); the install
profile's websocket URL (the console's build, after chronicle-45).
