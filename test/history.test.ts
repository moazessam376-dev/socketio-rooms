import { afterEach, describe, expect, it } from "vitest";
import type { Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  JoinAck,
  SendAck,
  ServerToClientEvents,
} from "../src/events.js";
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

describe("fresh room history", () => {
  afterEach(cleanup);

  it("returns the fifty most recent messages in ascending order", async () => {
    const running = await server({ prefix: uniquePrefix() });
    const sender = client(running.port, "sender");
    const room = "lobby";

    await once(sender, "server:hello");
    await expect(join(sender, room)).resolves.toMatchObject({ ok: true });

    for (let index = 1; index <= 60; index += 1) {
      await expect(
        send(sender, {
          room,
          text: `message-${index}`,
          clientId: `sender-${index}`,
        }),
      ).resolves.toEqual({ ok: true, seq: index });
    }

    const freshClient = client(running.port, "fresh");
    await once(freshClient, "server:hello");
    const ack = await join(freshClient, room);

    expect(ack).toMatchObject({ ok: true, gap: false });
    if (ack.ok) {
      expect(ack.missed.map((message) => message.seq)).toEqual(
        Array.from({ length: 50 }, (_, index) => index + 11),
      );
    }
  });
});
