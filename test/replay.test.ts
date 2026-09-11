import { afterEach, describe, expect, it } from "vitest";
import type { Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  JoinAck,
  SendAck,
  ServerToClientEvents,
} from "../src/events.js";
import { connectRedis, keys } from "../src/redis.js";
import {
  cleanup,
  client,
  once,
  server,
  uniquePrefix,
} from "./helpers.js";

type TestSocket = Socket<ServerToClientEvents, ClientToServerEvents>;

function join(
  socket: TestSocket,
  room: string,
  lastSeq?: number,
): Promise<JoinAck> {
  return new Promise((resolve) => {
    socket.emit("room:join", { room, lastSeq }, resolve);
  });
}

function send(
  socket: TestSocket,
  payload: { room: string; text: string; clientId: string },
): Promise<SendAck> {
  return new Promise((resolve) => {
    socket.emit("message:send", payload, resolve);
  });
}

async function reconnect(socket: TestSocket): Promise<void> {
  const disconnected = once<void>(socket, "disconnect");
  socket.disconnect();
  await disconnected;

  const connected = once<void>(socket, "connect");
  const hello = once<{ instanceId: string }>(socket, "server:hello");
  socket.connect();
  await Promise.all([connected, hello]);
}

describe("room replay", () => {
  afterEach(cleanup);

  it("returns messages sent while a client was away in sequence order", async () => {
    const running = await server({ prefix: uniquePrefix() });
    const alice = client(running.port, "alice");
    const bob = client(running.port, "bob");
    const room = "lobby";

    await Promise.all([
      once(alice, "server:hello"),
      once(bob, "server:hello"),
    ]);
    await expect(join(alice, room)).resolves.toMatchObject({ ok: true });
    await expect(join(bob, room)).resolves.toMatchObject({ ok: true });

    await reconnect(bob);
    await expect(
      send(alice, { room, text: "one", clientId: "alice-1" }),
    ).resolves.toEqual({ ok: true, seq: 1 });
    await expect(
      send(alice, { room, text: "two", clientId: "alice-2" }),
    ).resolves.toEqual({ ok: true, seq: 2 });
    await expect(
      send(alice, { room, text: "three", clientId: "alice-3" }),
    ).resolves.toEqual({ ok: true, seq: 3 });

    const ack = await join(bob, room, 0);

    expect(ack).toMatchObject({ ok: true, gap: false });
    if (ack.ok) {
      expect(ack.missed.map((message) => message.seq)).toEqual([1, 2, 3]);
    }
  });

  it("reports a gap when the requested history was trimmed", async () => {
    const running = await server({
      prefix: uniquePrefix(),
      bufferSize: 3,
    });
    const alice = client(running.port, "alice");
    const bob = client(running.port, "bob");
    const room = "lobby";

    await Promise.all([
      once(alice, "server:hello"),
      once(bob, "server:hello"),
    ]);
    await expect(join(alice, room)).resolves.toMatchObject({ ok: true });
    await expect(join(bob, room)).resolves.toMatchObject({ ok: true });

    await reconnect(bob);
    // MAXLEN ~ trims whole radix-tree nodes (100 entries each), so 250 sends
    // guarantee that the oldest entries are gone even with approximate trimming.
    for (let index = 1; index <= 250; index += 1) {
      await expect(
        send(alice, {
          room,
          text: `message-${index}`,
          clientId: `alice-${index}`,
        }),
      ).resolves.toEqual({ ok: true, seq: index });
    }

    const ack = await join(bob, room, 0);

    expect(ack).toMatchObject({ ok: true, gap: true });
    if (ack.ok) {
      const seqs = ack.missed.map((message) => message.seq);
      expect(seqs.length).toBeGreaterThan(0);
      expect(seqs[0]).toBeGreaterThan(1);
      expect(seqs[seqs.length - 1]).toBe(250);
      expect(seqs).toEqual([...seqs].sort((a, b) => a - b));
    }
  });

  it("replays based on the entries retained by approximate trimming", async () => {
    const prefix = uniquePrefix();
    const running = await server({ prefix, bufferSize: 3 });
    const alice = client(running.port, "alice");
    const bob = client(running.port, "bob");
    const room = "lobby";

    await Promise.all([
      once(alice, "server:hello"),
      once(bob, "server:hello"),
    ]);
    await expect(join(alice, room)).resolves.toMatchObject({ ok: true });
    await expect(join(bob, room)).resolves.toMatchObject({ ok: true });

    for (let index = 1; index <= 7; index += 1) {
      await expect(
        send(alice, {
          room,
          text: `message-${index}`,
          clientId: `alice-${index}`,
        }),
      ).resolves.toEqual({ ok: true, seq: index });
    }

    const redis = await connectRedis(process.env.REDIS_URL ?? "redis://localhost:6379");
    try {
      const streamLength = await redis.xLen(keys(prefix).room(room).stream);

      await reconnect(bob);
      const ack = await join(bob, room, 3);

      expect(ack).toMatchObject({ ok: true });
      if (ack.ok) {
        if (streamLength > 3) {
          expect(ack).toMatchObject({ gap: false });
          expect(ack.missed.map((message) => message.seq)).toEqual([4, 5, 6, 7]);
        } else {
          expect(ack).toMatchObject({ gap: true });
          expect(ack.missed.map((message) => message.seq)).toEqual(
            Array.from(
              { length: streamLength },
              (_value, index) => 7 - streamLength + index + 1,
            ),
          );
        }
      }
    } finally {
      await redis.quit();
    }
  });
});
