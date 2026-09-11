import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createConnection } from "node:net";
import { io, type Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  JoinAck,
  Message,
  SendAck,
  ServerToClientEvents,
} from "../src/events.js";

type DemoSocket = Socket<ServerToClientEvents, ClientToServerEvents>;

const prefix = `demo:${randomUUID()}`;
const children = new Set<ChildProcess>();
const clients = new Set<DemoSocket>();

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

function startInstance(port: number, instanceId: string): Promise<ChildProcess> {
  const child = spawn(process.execPath, ["--import", "tsx", "src/main.ts"], {
    env: {
      ...process.env,
      PORT: String(port),
      INSTANCE_ID: instanceId,
      ADAPTER: "1",
      PREFIX: prefix,
    },
    stdio: ["ignore", "pipe", "inherit"],
  });
  children.add(child);

  return new Promise((resolve, reject) => {
    const stdout = child.stdout;
    if (stdout === null) {
      reject(new Error(`could not watch stdout for instance ${instanceId}`));
      return;
    }

    let settled = false;
    let buffer = "";
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        reject(
          new Error(
            `instance ${instanceId} did not report listening on port ${port} within 10 seconds`,
          ),
        );
      }
    }, 10_000);

    stdout.setEncoding("utf8");
    stdout.on("data", (chunk: string) => {
      process.stdout.write(chunk);
      if (settled) {
        return;
      }

      buffer += chunk;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      const readyLine = `listening on ${port} as ${instanceId}`;
      if (lines.some((line) => line.trim() === readyLine)) {
        settled = true;
        clearTimeout(timer);
        resolve(child);
      }
    });

    child.once("error", (error) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(error);
      }
    });
    child.once("exit", (code, signal) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(
          new Error(
            `instance ${instanceId} exited before readiness with ${
              signal ?? `code ${code ?? "unknown"}`
            }`,
          ),
        );
      }
    });
  });
}

function waitForExit(child: ChildProcess, timeoutMs: number): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    const timer = setTimeout(resolve, timeoutMs);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function waitForPortFree(port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const refused = await new Promise<boolean>((resolve, reject) => {
      const connection = createConnection({ host: "localhost", port });
      let settled = false;
      const settle = (result: () => void): void => {
        if (settled) {
          return;
        }
        settled = true;
        connection.destroy();
        result();
      };

      connection.once("connect", () => settle(() => resolve(false)));
      connection.once("timeout", () => settle(() => resolve(false)));
      connection.once("error", (error) => {
        const code = (error as NodeJS.ErrnoException).code;
        if (code === "ECONNREFUSED") {
          settle(() => resolve(true));
          return;
        }
        settle(() => reject(error));
      });
      connection.setTimeout(100);
    });

    if (refused) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  throw new Error(`port ${port} did not become free within ${timeoutMs}ms`);
}

async function stopChildren(): Promise<void> {
  for (const child of children) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGTERM");
    }
  }

  await Promise.all([...children].map((child) => waitForExit(child, 2_000)));
  for (const child of children) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
    }
  }
  await Promise.all([...children].map((child) => waitForExit(child, 2_000)));
}

function waitForConnect(socket: DemoSocket, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("client did not connect in time")),
      timeoutMs,
    );
    socket.once("connect", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

function waitForDisconnect(socket: DemoSocket, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("client did not disconnect in time")),
      timeoutMs,
    );
    socket.once("disconnect", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

function waitForRecovered(socket: DemoSocket, timeoutMs: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("client 1 did not recover in time")),
      timeoutMs,
    );
    socket.on("connect", () => {
      if (socket.recovered) {
        clearTimeout(timer);
        resolve(Date.now());
      }
    });
  });
}

function connectClient(
  port: number,
  name: string,
  reconnect = false,
): DemoSocket {
  const socket = io(`http://localhost:${port}`, {
    auth: { name },
    autoConnect: false,
    reconnectionDelay: 500,
    reconnectionDelayMax: 2_000,
    randomizationFactor: 0,
  }) as DemoSocket;
  clients.add(socket);
  socket.connect();
  return socket;
}

function join(socket: DemoSocket, room: string, lastSeq?: number): Promise<JoinAck> {
  return new Promise((resolve) => {
    socket.emit("room:join", lastSeq === undefined ? { room } : { room, lastSeq }, resolve);
  });
}

function send(
  socket: DemoSocket,
  payload: { room: string; text: string; clientId: string },
): Promise<SendAck> {
  return new Promise((resolve) => {
    socket.emit("message:send", payload, resolve);
  });
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
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

async function waitUntil(
  predicate: () => boolean,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`${label} timed out`);
}

async function sendAndObserve(
  socket: DemoSocket,
  received: Message[],
  room: string,
  text: string,
  seq: number,
  clientId: string,
): Promise<void> {
  const ack = await withTimeout(
    send(socket, { room, text, clientId }),
    10_000,
    `send ${clientId}`,
  );
  assert(ack.ok && ack.seq === seq, `expected ${clientId} to receive seq ${seq}`);
  await waitUntil(
    () => received.some((message) => message.seq === seq),
    10_000,
    `client 1 message seq ${seq}`,
  );
}

async function runDemo(): Promise<void> {
  const first = await startInstance(3001, "a");
  const second = await startInstance(3002, "b");
  const room = "demo";
  const receivedByClientOne: Message[] = [];
  const receivedByClientTwo: Message[] = [];
  const clientOne = connectClient(3001, "alice", true);
  const clientTwo = connectClient(3002, "bob");
  clientOne.on("message", (message) => receivedByClientOne.push(message));
  clientTwo.on("message", (message) => receivedByClientTwo.push(message));

  await Promise.all([
    waitForConnect(clientOne, 10_000),
    waitForConnect(clientTwo, 10_000),
  ]);
  const firstJoin = await withTimeout(join(clientOne, room), 10_000, "client 1 join");
  const secondJoin = await withTimeout(join(clientTwo, room), 10_000, "client 2 join");
  assert(firstJoin.ok && secondJoin.ok, "initial room join failed");

  for (let index = 1; index <= 3; index += 1) {
    await sendAndObserve(
      clientOne,
      receivedByClientOne,
      room,
      `message-${index}`,
      index,
      `alice-${index}`,
    );
  }
  for (let index = 4; index <= 6; index += 1) {
    await sendAndObserve(
      clientTwo,
      receivedByClientOne,
      room,
      `message-${index}`,
      index,
      `bob-${index}`,
    );
  }

  const recoveredAtPromise = waitForRecovered(clientOne, 30_000);
  const disconnected = waitForDisconnect(clientOne, 10_000);
  clientOne.io.engine.close();
  await disconnected;
  await new Promise((resolve) => setTimeout(resolve, 100));

  const killedAt = Date.now();
  first.kill("SIGKILL");
  await waitForExit(first, 5_000);
  await waitForPortFree(3001, 5_000);

  const replacement = await startInstance(3001, "a2");
  assert(!clientOne.connected, "client 1 reconnected before the failover message");
  const failoverAck = await withTimeout(
    send(clientTwo, {
      room,
      text: "message-7",
      clientId: "bob-7",
    }),
    10_000,
    "send bob-7",
  );
  assert(failoverAck.ok && failoverAck.seq === 7, "expected bob-7 to receive seq 7");

  const recoveredAt = await recoveredAtPromise;
  const reconnectMsA = recoveredAt - killedAt;
  const recoveredA = clientOne.recovered;
  assert(recoveredA, "client 1 did not recover via connection state recovery");
  await waitUntil(
    () => receivedByClientOne.some((message) => message.seq === 7),
    10_000,
    "client 1 message seq 7",
  );
  await sendAndObserve(
    clientTwo,
    receivedByClientOne,
    room,
    "message-8",
    8,
    "bob-8",
  );
  await sendAndObserve(
    clientOne,
    receivedByClientOne,
    room,
    "message-9",
    9,
    "alice-9",
  );

  const presence = await withTimeout(join(clientOne, room), 10_000, "presence sweep join");
  assert(presence.ok, "presence sweep join failed");
  assert(presence.members.length === 2, "presence did not contain exactly two members");
  const instanceIds = new Set(presence.members.map((member) => member.instanceId));
  assert(
    instanceIds.size === 2 && instanceIds.has("a2") && instanceIds.has("b"),
    "presence did not contain instances a2 and b",
  );

  const counts = new Map<number, number>();
  for (const message of receivedByClientOne) {
    counts.set(message.seq, (counts.get(message.seq) ?? 0) + 1);
  }
  const sequences = [...counts.keys()].sort((left, right) => left - right);
  assert(
    sequences.length === 9 && sequences.every((seq, index) => seq === index + 1),
    `client 1 received unexpected sequences: ${sequences.join(", ")}`,
  );
  for (let seq = 1; seq <= 9; seq += 1) {
    assert(counts.get(seq) === 1, `client 1 received seq ${seq} more than once`);
  }

  const messageCountA = receivedByClientOne.length;

  const lastSeqBeforeCrash = Math.max(
    ...receivedByClientTwo.map((message) => message.seq),
  );
  assert(lastSeqBeforeCrash === 9, "client 2 did not receive scenario A messages");

  const disconnectedB = waitForDisconnect(clientTwo, 10_000);
  const killedAtB = Date.now();
  second.kill("SIGKILL");
  await disconnectedB;
  await waitForExit(second, 5_000);
  await waitForPortFree(3002, 5_000);

  const reconnectPromiseB = waitForConnect(clientTwo, 30_000);
  await startInstance(3002, "b2");
  const sendAlice = (seq: number) =>
    sendAndObserve(clientOne, receivedByClientOne, room, `message-${seq}`, seq, `alice-${seq}`);
  for (let seq = lastSeqBeforeCrash + 1; seq <= lastSeqBeforeCrash + 2; seq += 1) {
    await sendAlice(seq);
  }

  await reconnectPromiseB;
  const reconnectMsB = Date.now() - killedAtB;
  const recoveredB = clientTwo.recovered;
  assert(!recoveredB, "client 2 unexpectedly recovered connection state after a crash");

  const rejoinAck = await withTimeout(
    join(clientTwo, room, lastSeqBeforeCrash),
    10_000,
    "client 2 rejoin",
  );
  assert(rejoinAck.ok, "client 2 rejoin failed");
  assert(!rejoinAck.gap, "client 2 rejoin reported a message gap");
  assert(
    rejoinAck.missed.length === 2 &&
      rejoinAck.missed[0]?.seq === lastSeqBeforeCrash + 1 &&
      rejoinAck.missed[1]?.seq === lastSeqBeforeCrash + 2,
    "client 2 rejoin returned unexpected missed messages",
  );
  receivedByClientTwo.push(...rejoinAck.missed);

  const sendBob = (seq: number) =>
    sendAndObserve(clientTwo, receivedByClientTwo, room, `message-${seq}`, seq, `bob-${seq}`);
  for (let seq = lastSeqBeforeCrash + 3; seq <= lastSeqBeforeCrash + 5; seq += 1) {
    await sendBob(seq);
  }

  const countsB = new Map<number, number>();
  for (const message of receivedByClientTwo) {
    countsB.set(message.seq, (countsB.get(message.seq) ?? 0) + 1);
  }
  const sequencesB = [...countsB.keys()].sort((left, right) => left - right);
  assert(
    sequencesB.length === lastSeqBeforeCrash + 5 &&
      sequencesB.every((seq, index) => seq === index + 1),
    `client 2 received unexpected sequences: ${sequencesB.join(", ")}`,
  );
  for (let seq = 1; seq <= lastSeqBeforeCrash + 5; seq += 1) {
    assert(countsB.get(seq) === 1, `client 2 received seq ${seq} more than once`);
  }
  const messageCountB = receivedByClientTwo.length;

  console.log("scenario A: clean disconnect, then kill and replace");
  console.log("metric       value");
  console.log("scenario     A");
  console.log(`recovered    ${recoveredA}`);
  console.log(`reconnectMs  ${reconnectMsA}`);
  console.log(`messageCount ${messageCountA}`);
  console.log("");
  console.log("scenario B: crash with the client attached");
  console.log("metric       value");
  console.log("scenario     B");
  console.log(`recovered    ${recoveredB}`);
  console.log(`reconnectMs  ${reconnectMsB}`);
  console.log(`messageCount ${messageCountB}`);
  console.log("");
  console.log("reconnectMs is bounded by the client's reconnectionDelay, not a measured property");

  void replacement;
}

let exitCode = 0;
try {
  await runDemo();
} catch (error) {
  exitCode = 1;
  console.error(`demo failed: ${error instanceof Error ? error.message : String(error)}`);
} finally {
  for (const socket of clients) {
    socket.disconnect();
  }
  await stopChildren();
}

process.exit(exitCode);
