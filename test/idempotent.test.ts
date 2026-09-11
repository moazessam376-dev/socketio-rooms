import { afterEach, describe, expect, it } from "vitest";
import type { Socket } from "socket.io-client";
import { connectRedis, keys } from "../src/redis.js";
import type {
  ClientToServerEvents,
  ServerToClientEvents,
  SendAck,
} from "../src/events.js";
import {
  cleanup,
  client,
  once,
  server,
  uniquePrefix,
} from "./helpers.js";

type TestSocket = Socket<ServerToClientEvents, ClientToServerEvents>;

function join(socket: TestSocket, room: string): Promise<unknown> {
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

describe("idempotent message append", () => {
  let redis: Awaited<ReturnType<typeof connectRedis>> | undefined;

  afterEach(async () => {
    await cleanup();
    await redis?.quit();
    redis = undefined;
  });

  it("deduplicates clientIds and assigns consecutive sequence numbers", async () => {
    const prefix = uniquePrefix();
    const running = await server({ prefix });
    redis = await connectRedis(process.env.REDIS_URL ?? "redis://localhost:6379");
    const socket = client(running.port, "alice");
    await once(socket, "server:hello");
    await join(socket, "lobby");

    const payload = {
      room: "lobby",
      text: "hello",
      clientId: "client-1",
    };
    const first = await send(socket, payload);
    const second = await send(socket, payload);

    expect(first).toEqual({ ok: true, seq: 1 });
    expect(second).toEqual(first);

    const stream = keys(prefix).room("lobby").stream;
    await expect(redis.xLen(stream)).resolves.toBe(1);

    const third = await send(socket, {
      room: "lobby",
      text: "second",
      clientId: "client-2",
    });
    const fourth = await send(socket, {
      room: "lobby",
      text: "third",
      clientId: "client-3",
    });
    expect(third).toEqual({ ok: true, seq: 2 });
    expect(fourth).toEqual({ ok: true, seq: 3 });

    await expect(
      send(socket, {
        room: "bad room",
        text: "nope",
        clientId: "client-4",
      }),
    ).resolves.toEqual({ ok: false, error: expect.any(String) });
  });
});
