import { afterEach, describe, expect, it } from "vitest";
import type { Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  JoinAck,
  SendAck,
  ServerToClientEvents,
} from "../src/events.js";
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
    for (let index = 1; index <= 5; index += 1) {
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
      expect(ack.missed.map((message) => message.seq)).toEqual([3, 4, 5]);
    }
  });
});
