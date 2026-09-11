# socketio-rooms

socketio-rooms is a Socket.IO server for named chat rooms with presence and ordered messages. Redis stores the room buffer, presence records, and idempotency keys.
It supports connection state recovery, room replay, and multiple server instances.

## How to run

The server expects Redis at `redis://localhost:6379` by default. Set `REDIS_URL` to use another Redis URL.

With Homebrew:

```sh
brew install redis
brew services start redis
```

With Docker:

```sh
docker run --name socketio-rooms-redis -p 6379:6379 redis:7
```

Install the dependencies and start one server:

```sh
npm install
npm run dev
```

To start two servers on ports 3001 and 3002, use:

```sh
npm run dev:two
```

The failover demo starts its own child processes and clients:

```sh
npm run demo
```

## Events

Client to server events use these payloads and acknowledgements.

| Event | Payload | Acknowledgement |
| --- | --- | --- |
| `room:join` | `{ room: string, lastSeq?: number }` | `{ ok, members, missed, gap }` or `{ ok: false, error }` |
| `room:leave` | `{ room: string }` | `{ ok: boolean }` |
| `message:send` | `{ room: string, text: string, clientId: string }` | `{ ok: true, seq }` or `{ ok: false, error }` |
| `typing` | `{ room: string, on: boolean }` | none |

Server to client events are:

| Event | Payload |
| --- | --- |
| `server:hello` | `{ instanceId: string }` |
| `presence` | `{ room: string, members: Member[] }` where each member has `socketId`, `name`, and `instanceId` |
| `message` | `{ room, seq, from, text, clientId, ts }` |
| `typing` | `{ room: string, from: string, on: boolean }` |

## Reconnection

The first layer is Socket.IO connection state recovery for up to 120 seconds. It restores the socket rooms and replays packets when the session and adapter stream are available.

The second layer is room replay. When recovery is not available, the client joins each remembered room with its last received sequence number. The server returns messages after that sequence and reports `gap: true` if the bounded buffer no longer contains all of them. A fresh join without `lastSeq` returns the most recent fifty messages with `gap: false`.

Messages submitted by the browser first enter an outbox keyed by `clientId`. The browser retries only while connected, waits in the outbox while disconnected, and sends each waiting message once after reconnect. After three failed attempts the item remains visible as `not sent` with a retry control that uses the same `clientId`.

## Scaling and failover

When `ADAPTER=1`, each server uses the Redis Streams adapter with a separate Redis connection. The stream and session keys use the configured prefix, so two servers with the same prefix share fan-out and recovery data.

Sticky sessions are required for the HTTP long-polling handshake when more than one instance is behind a load balancer. WebSocket-only transport avoids that handshake requirement, but requires proxies that allow WebSocket connections.

Failover means starting a new instance on the same port after the old instance stops. Clients keep their configured URL, and the replacement instance uses the shared adapter stream and session data. Which recovery layer a client takes depends on how it lost the old instance. Socket.IO persists a recovery session only when a socket disconnects cleanly, so a client that was still attached when the instance died reconnects with `socket.recovered` false and takes layer two: it re-joins with its last sequence number and receives what it missed. A client that had disconnected cleanly before the instance died recovers through layer one. `npm run demo` is a separate script that shows both paths; the test suite does not run it.

## Tests

The tests cover RoomStore behavior, integration behavior against real Redis, cluster fan-out, cross-instance recovery, and ghost presence cleanup. The process-level demo is a separate script, `npm run demo`.

CI runs the typecheck, client build, and Vitest suite with a Redis 7 service.

## Not in scope

Persistence beyond the buffer, identity, rate limiting, TLS, and deployment.
