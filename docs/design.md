# socketio-rooms design

Chat rooms over Socket.IO. Clients join rooms, see who is present, send messages
that get a server-assigned sequence number, and survive disconnects and instance
crashes without losing messages. Built 2026-09 as a practice project;

## Decisions

Each decision: chosen / rejected / why here / what breaks if wrong.

1. Chat rooms, not a live dashboard. Rejected: one-way metrics push. Rooms,
   presence, acknowledgements and replay give more surface to reason about.
   If wrong: the project looks like a toy chat; the mitigation is the
   reconnection and scaling story, which a dashboard would not exercise.
2. Socket.IO, not raw ws. Rejected: `ws` with a hand-written protocol.
   Socket.IO gives rooms, acknowledgements, automatic reconnection with backoff,
   connection state recovery, and adapters for multi-instance fan-out.
   Cost: its own framing on top of WebSocket, so plain WebSocket clients cannot
   connect, and the long-polling fallback needs sticky sessions behind a load
   balancer. If wrong: a client outside the JavaScript ecosystem has to use a
   Socket.IO client library.
3. Redis Streams adapter, not the pub/sub adapter. Rejected:
   `@socket.io/redis-adapter` (pub/sub) and the cluster adapter. The Streams
   adapter persists sessions and event offsets in Redis, so connection state
   recovery works when the client lands on a different instance. Pub/sub keeps
   nothing, so recovery only works on the same instance. If wrong: recovery
   silently degrades to a full rejoin after failover; layer two below catches it.
4. Server-assigned per-room sequence from Redis `INCR`. Rejected: server
   timestamps (not unique, clocks differ across instances) and client-side ids
   (clients cannot order each other). `INCR` is atomic across instances.
   If wrong: replay cannot say "after seq N" and gaps are undetectable.
5. Bounded Redis stream per room, 500 entries, `XADD MAXLEN ~`. Rejected:
   unbounded stream (memory grows forever) and a SQL table (a second store for a
   practice project). Replay serves from the stream and reports `gap: true`
   when the requested `lastSeq` is older than the oldest entry. If wrong:
   a client offline longer than 500 messages sees a truncated history, which the
   client shows as a notice rather than pretending nothing was missed.
6. Two-layer reconnection. Layer one: Socket.IO connection state recovery,
   window two minutes, `skipMiddlewares: true`, backed by the adapter's session
   store. Layer two: if `socket.recovered` is false after reconnect, the client
   re-joins each room with its `lastSeq` and the server returns the missed
   messages in order. Rejected: layer one alone (fails past the window, or when
   the session store is gone) and layer two alone (loses room membership and
   makes every reconnect a full rejoin). If wrong: duplicates or gaps; the
   client dedupes on `seq`, and the demo measures the recovery time.
7. Idempotent send through a client-generated `clientId`. The append is one
   atomic Redis script: read the `clientId` key, else `INCR`, `XADD`, and
   `SET NX EX 300`. Rejected: get-then-set from Node (two clients or a retry
   can interleave between the get and the set and produce two sequence
   numbers for one message). Client emits with a five-second acknowledgement
   timeout and retries up to three times with the same `clientId`. If wrong:
   a lost acknowledgement becomes a duplicate message.
8. Presence in a Redis hash per room plus an instance heartbeat key with a
   ten-second TTL. Members whose instance key expired are dropped on read and
   removed by a sweep. Rejected: `io.in(room).fetchSockets()` across the
   adapter, which is a round trip to every instance with a timeout on every
   read and cannot tell a slow instance from a dead one. If wrong: a crashed
   instance leaves ghost members for up to ten seconds, which the doc states.
9. Auth is a validated display name in the handshake (`auth.name`, 1 to 32
   characters, letters, digits, underscore, hyphen), checked in `io.use`.
   Rejected for this project: signed tokens. The generalisation is the same
   middleware verifying a JWT and setting `socket.data.user`; the shape of the
   code does not change. If wrong: anyone can pick any name; there is no
   identity claim here and the README says so.
10. Failover in the demo is "a new instance on the same port". The client is
    bound to one URL, which is what a load balancer gives it. Rejected: a
    client-side URL list (a real deployment does not expose instance ports).
    Sticky sessions are required for the HTTP long-polling handshake when more
    than one instance sits behind a balancer; WebSocket-only transport avoids
    that at the cost of proxies that block WebSocket.
11. Tests against real Redis, not mocks. A mock cannot show that `INCR`,
    `XADD MAXLEN` and `SET NX` behave atomically together. CI runs a
    `redis:7` service container.
12. Static client with no framework, built by esbuild to one file. The project
    is about the socket layer; a framework would add nothing to that story.

## Numbers

Buffer 500 messages per room. Recovery window 120 s. Heartbeat TTL 10 s,
refreshed every 3 s. `clientId` key TTL 300 s. Acknowledgement timeout 5 s,
three retries.

## Not in scope

Persistence beyond the buffer, identity, rate limiting, TLS, deployment.
