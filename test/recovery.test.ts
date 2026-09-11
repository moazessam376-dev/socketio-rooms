import { afterEach, describe, expect, it } from "vitest";
import type { Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  JoinAck,
  Message,
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
import type { RunningServer } from "../src/server.js";

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

async function drop(socket: TestSocket): Promise<void> {
  const disconnected = once<void>(socket, "disconnect");
  socket.io.engine.close();
  await disconnected;
}

async function waitForMemberToLeave(
  running: RunningServer,
  room: string,
  name: string,
): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const members = await running.store.members(room);
    if (!members.some((member) => member.name === name)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`${name} did not leave ${room}`);
}

describe("connection state recovery", () => {
  afterEach(cleanup);

  it("delivers a missed message through Socket.IO recovery", async () => {
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

    const reconnect = new Promise<boolean>((resolve) => {
      alice.once("connect", () => resolve(alice.recovered));
    });
    const missed = once<Message>(alice, "message");
    await drop(alice);
    await waitForMemberToLeave(running, room, "alice");

    await expect(
      send(bob, {
        room,
        text: "during recovery",
        clientId: "bob-1",
      }),
    ).resolves.toEqual({ ok: true, seq: 1 });

    await expect(reconnect).resolves.toBe(true);
    await expect(missed).resolves.toMatchObject({
      room,
      seq: 1,
      text: "during recovery",
    });
  });

  it("falls back to room replay when recovery is disabled", async () => {
    const running = await server({
      prefix: uniquePrefix(),
      recovery: false,
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

    const reconnect = new Promise<boolean>((resolve) => {
      alice.once("connect", () => resolve(alice.recovered));
    });
    await drop(alice);
    await waitForMemberToLeave(running, room, "alice");

    await expect(
      send(bob, {
        room,
        text: "during replay",
        clientId: "bob-1",
      }),
    ).resolves.toEqual({ ok: true, seq: 1 });

    await expect(reconnect).resolves.toBeFalsy();
    const ack = await join(alice, room, 0);

    expect(ack).toMatchObject({ ok: true, gap: false });
    if (ack.ok) {
      expect(ack.missed.map((message) => message.seq)).toEqual([1]);
      expect(ack.missed[0]?.text).toBe("during replay");
    }
  });
});
