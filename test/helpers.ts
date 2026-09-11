import { randomUUID } from "node:crypto";
import type { ManagerOptions, SocketOptions } from "socket.io-client";
import { io, Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  ServerToClientEvents,
} from "../src/events.js";
import {
  startServer,
  type RunningServer,
  type ServerOptions,
} from "../src/server.js";

const servers = new Set<RunningServer>();
const clients = new Set<Socket<ServerToClientEvents, ClientToServerEvents>>();

export function uniquePrefix(): string {
  return `test:${randomUUID()}`;
}

export async function server(
  over: Partial<ServerOptions> = {},
): Promise<RunningServer> {
  const running = await startServer({
    redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379",
    prefix: uniquePrefix(),
    instanceId: `test-instance-${randomUUID()}`,
    port: 0,
    ...over,
  });
  servers.add(running);
  return running;
}

export function client(
  port: number,
  name: string,
  extra: Partial<ManagerOptions & SocketOptions> = {},
): Socket<ServerToClientEvents, ClientToServerEvents> {
  const socket = io(
    `http://localhost:${port}`,
    {
      auth: { name },
      ...extra,
    },
  ) as Socket<ServerToClientEvents, ClientToServerEvents>;
  clients.add(socket);
  return socket;
}

export function once<T>(socket: any, event: string): Promise<T> {
  return new Promise<T>((resolve) => {
    socket.once(event, (value: T) => resolve(value));
  });
}

export async function cleanup(): Promise<void> {
  for (const socket of clients) {
    socket.disconnect();
  }
  clients.clear();

  for (const running of servers) {
    if (!running.closed) {
      await running.store.clear();
    }
    await running.close();
  }
  servers.clear();
}
