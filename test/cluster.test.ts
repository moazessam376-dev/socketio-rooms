import { afterEach, describe, expect, it } from "vitest";
import type { Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  JoinAck,
  Message,
  SendAck,
  ServerToClientEvents,
} from "../src/events.js";
import { connectRedis, keys } from "../src/redis.js";
import { cleanup, client, once, server, uniquePrefix } from "./helpers.js";

type TestSocket = Socket<ServerToClientEvents, ClientToServerEvents>;

function join(socket: TestSocket, room: string): Promise<JoinAck> {
  return new Promise((resolve) => {
    socket.emit("room:join", { room }, resolve);
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

async function waitFor<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

describe("multi-instance Socket.IO behavior", () => {
  afterEach(cleanup);

  it("fans messages out to a client on another instance", async () => {
    const prefix = uniquePrefix();
    const first = await server({ prefix, adapter: true });
    const second = await server({ prefix, adapter: true });
    const firstClient = client(first.port, "alice");
    const secondClient = client(second.port, "bob");
    const room = "lobby";

    await Promise.all([
      once(firstClient, "server:hello"),
      once(secondClient, "server:hello"),
    ]);
    await expect(join(firstClient, room)).resolves.toMatchObject({ ok: true });
    await expect(join(secondClient, room)).resolves.toMatchObject({ ok: true });

    const received = once<Message>(secondClient, "message");
    await expect(
      send(firstClient, {
        room,
        text: "hello from instance one",
        clientId: "alice-1",
      }),
    ).resolves.toEqual({ ok: true, seq: 1 });

    await expect(received).resolves.toMatchObject({
      room,
      seq: 1,
      from: "alice",
      text: "hello from instance one",
    });
  });

  it(
    "recovers a client and replays a message sent through the replacement instance",
    async () => {
      const prefix = uniquePrefix();
      const first = await server({
        prefix,
        adapter: true,
        instanceId: "a",
      });
      const firstPort = first.port;
      const original = client(firstPort, "alice", {
        reconnectionDelay: 3000,
        reconnectionDelayMax: 3000,
        randomizationFactor: 0,
      });
      const room = "lobby";

      await once(original, "server:hello");
      await expect(join(original, room)).resolves.toMatchObject({ ok: true });

      const recovered = waitFor(
        new Promise<void>((resolve) => {
          original.on("connect", () => {
            if (original.recovered) {
              resolve();
            }
          });
        }),
        15_000,
        "connection state recovery",
      );
      const received = once<Message>(original, "message");
      const disconnected = once<string>(original, "disconnect");
      await first.close();
      await disconnected;

      const replacement = await server({
        prefix,
        adapter: true,
        instanceId: "b",
        port: firstPort,
      });
      const secondClient = client(replacement.port, "bob");
      await once(secondClient, "server:hello");
      await expect(join(secondClient, room)).resolves.toMatchObject({ ok: true });

      expect(original.connected).toBe(false);
      await expect(
        send(secondClient, {
          room,
          text: "sent during failover",
          clientId: "bob-1",
        }),
      ).resolves.toEqual({ ok: true, seq: 1 });

      await expect(recovered).resolves.toBeUndefined();
      await expect(received).resolves.toMatchObject({
        room,
        seq: 1,
        text: "sent during failover",
      });
    },
    25_000,
  );

  it("filters expired instance members and sweep removes their hash entries", async () => {
    const prefix = uniquePrefix();
    const first = await server({
      prefix,
      adapter: true,
      instanceId: "a",
      instanceTtlSeconds: 1,
    });
    const second = await server({
      prefix,
      adapter: true,
      instanceId: "b",
      instanceTtlSeconds: 1,
    });
    const room = "lobby";
    const ghost = {
      socketId: "ghost-socket",
      name: "ghost",
      instanceId: first.instanceId,
    };

    await expect(first.store.join(room, ghost)).resolves.toEqual([ghost]);
    await expect(second.store.members(room)).resolves.toEqual([ghost]);

    await first.close();
    await new Promise((resolve) => setTimeout(resolve, 1_500));

    await expect(second.store.members(room)).resolves.toEqual([]);
    await expect(second.store.sweep(room)).resolves.toEqual([]);

    const redis = await connectRedis(process.env.REDIS_URL ?? "redis://localhost:6379");
    try {
      await expect(redis.hGetAll(keys(prefix).room(room).members)).resolves.toEqual({});
    } finally {
      await redis.quit();
    }
  });
});
