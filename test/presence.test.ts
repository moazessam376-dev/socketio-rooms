import { afterEach, describe, expect, it } from "vitest";
import type { JoinAck } from "../src/events.js";
import { cleanup, client, once, server, uniquePrefix } from "./helpers.js";
import type { Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  ServerToClientEvents,
} from "../src/events.js";

type TestSocket = Socket<ServerToClientEvents, ClientToServerEvents>;

function join(socket: TestSocket, room: string): Promise<JoinAck> {
  return new Promise((resolve) => {
    socket.emit("room:join", { room }, resolve);
  });
}

function leave(socket: TestSocket, room: string): Promise<{ ok: boolean }> {
  return new Promise((resolve) => {
    socket.emit("room:leave", { room }, resolve);
  });
}

describe("room presence", () => {
  afterEach(cleanup);

  it("broadcasts joins, leaves, and disconnects", async () => {
    const running = await server({ prefix: uniquePrefix() });
    const alice = client(running.port, "alice");
    const bob = client(running.port, "bob");
    const room = "lobby";

    await Promise.all([
      once(alice, "server:hello"),
      once(bob, "server:hello"),
    ]);

    const alicePresence = once<{ room: string; members: unknown[] }>(
      alice,
      "presence",
    );
    const aliceJoin = await join(alice, room);
    expect(aliceJoin).toMatchObject({ ok: true });
    await alicePresence;

    const bothPresence = [
      once<{ room: string; members: Array<{ name: string }> }>(alice, "presence"),
      once<{ room: string; members: Array<{ name: string }> }>(bob, "presence"),
    ];
    const bobJoin = await join(bob, room);
    expect(bobJoin).toMatchObject({ ok: true });
    const [aliceUpdate, bobUpdate] = await Promise.all(bothPresence);
    expect(aliceUpdate.members).toHaveLength(2);
    expect(bobUpdate.members).toHaveLength(2);
    expect(aliceUpdate.members.map((member) => member.name)).toEqual(
      expect.arrayContaining(["alice", "bob"]),
    );

    const afterLeave = once<{ room: string; members: Array<{ name: string }> }>(
      alice,
      "presence",
    );
    await expect(leave(bob, room)).resolves.toEqual({ ok: true });
    const leaveUpdate = await afterLeave;
    expect(leaveUpdate.members).toHaveLength(1);
    expect(leaveUpdate.members[0]?.name).toBe("alice");

    const carol = client(running.port, "carol");
    await once(carol, "server:hello");
    const carolJoinPresence = once(alice, "presence");
    await expect(join(carol, room)).resolves.toMatchObject({ ok: true });
    await carolJoinPresence;

    const afterDisconnect = once<{
      room: string;
      members: Array<{ name: string }>;
    }>(alice, "presence");
    carol.disconnect();
    const disconnectUpdate = await afterDisconnect;
    expect(disconnectUpdate.members).toHaveLength(1);
    expect(disconnectUpdate.members[0]?.name).toBe("alice");
  });
});
