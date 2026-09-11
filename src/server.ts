import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { Server } from "socket.io";
import type { Socket } from "socket.io";
import {
  joinSchema,
  leaveSchema,
  sendSchema,
  typingSchema,
  type ClientToServerEvents,
  type Message,
  type ServerToClientEvents,
  type SocketData,
} from "./events.js";
import { parseName } from "./auth.js";
import { connectRedis } from "./redis.js";
import { RoomStore } from "./rooms.js";

export type ServerOptions = {
  redisUrl: string;
  prefix?: string;
  instanceId: string;
  port?: number;
  recovery?: {
    maxDisconnectionDuration?: number;
    skipMiddlewares?: boolean;
  } | false;
  adapter?: boolean;
  serveStatic?: boolean;
  bufferSize?: number;
};

export type RunningServer = {
  io: Server<
    ClientToServerEvents,
    ServerToClientEvents,
    Record<string, never>,
    SocketData
  >;
  store: RoomStore;
  port: number;
  instanceId: string;
  close(): Promise<void>;
};

type RoomSocket = Socket<
  ClientToServerEvents,
  ServerToClientEvents,
  Record<string, never>,
  SocketData
>;

function errorText(error: unknown, fallback: string): string {
  return error instanceof Error && error.message.length > 0 ? error.message : fallback;
}

export async function startServer(opts: ServerOptions): Promise<RunningServer> {
  const redis = await connectRedis(opts.redisUrl);
  const store = new RoomStore(redis, {
    prefix: opts.prefix ?? "socketio-rooms",
    instanceId: opts.instanceId,
    bufferSize: opts.bufferSize,
  });
  const httpServer = createServer((request, response) => {
    if (opts.serveStatic !== true) {
      response.statusCode = 404;
      response.end();
      return;
    }

    const pathname = (request.url ?? "/").split("?", 1)[0];
    const file =
      pathname === "/"
        ? {
            path: new URL("../public/index.html", import.meta.url),
            contentType: "text/html; charset=utf-8",
          }
        : pathname === "/app.js"
          ? {
              path: new URL("../public/app.js", import.meta.url),
              contentType: "application/javascript; charset=utf-8",
            }
          : undefined;

    if (file === undefined) {
      response.statusCode = 404;
      response.end();
      return;
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      response.statusCode = 405;
      response.setHeader("Allow", "GET, HEAD");
      response.end();
      return;
    }

    void readFile(file.path)
      .then((contents) => {
        response.statusCode = 200;
        response.setHeader("Content-Type", file.contentType);
        if (request.method === "HEAD") {
          response.end();
          return;
        }
        response.end(contents);
      })
      .catch(() => {
        response.statusCode = 404;
        response.end("Not found");
      });
  });
  const connectionStateRecovery =
    opts.recovery === false
      ? undefined
      : {
          maxDisconnectionDuration:
            opts.recovery?.maxDisconnectionDuration ?? 120_000,
          skipMiddlewares: opts.recovery?.skipMiddlewares ?? true,
        };
  const io = new Server<
    ClientToServerEvents,
    ServerToClientEvents,
    Record<string, never>,
    SocketData
  >(httpServer, {
    ...(connectionStateRecovery === undefined
      ? {}
      : { connectionStateRecovery }),
  });

  io.use((socket, next) => {
    const name = parseName(socket.handshake.auth);
    if (name === null) {
      next(new Error("invalid name"));
      return;
    }

    socket.data.name = name;
    next();
  });

  io.on("connection", (socket: RoomSocket) => {
    const joinedRooms = new Set(
      [...socket.rooms].filter((room) => room !== socket.id),
    );
    socket.emit("server:hello", { instanceId: opts.instanceId });

    if (socket.recovered) {
      void (async () => {
        for (const room of joinedRooms) {
          const members = await store.join(room, {
            socketId: socket.id,
            name: socket.data.name,
            instanceId: opts.instanceId,
          });
          io.to(room).emit("presence", { room, members });
        }
      })().catch(() => undefined);
    }

    socket.on("room:join", async (payload, ack) => {
      const parsed = joinSchema.safeParse(payload);
      if (!parsed.success) {
        ack({ ok: false, error: parsed.error.issues[0]?.message ?? "invalid payload" });
        return;
      }

      const { room, lastSeq } = parsed.data;
      try {
        let missed: Message[] = [];
        let gap = false;
        if (lastSeq !== undefined) {
          ({ missed, gap } = await store.after(room, lastSeq));
        }

        const members = await store.join(room, {
          socketId: socket.id,
          name: socket.data.name,
          instanceId: opts.instanceId,
        });
        await socket.join(room);
        joinedRooms.add(room);
        io.to(room).emit("presence", { room, members });
        ack({ ok: true, members, missed, gap });
      } catch (error) {
        ack({ ok: false, error: errorText(error, "join failed") });
      }
    });

    socket.on("room:leave", async (payload, ack) => {
      const parsed = leaveSchema.safeParse(payload);
      if (!parsed.success) {
        ack({ ok: false });
        return;
      }

      const { room } = parsed.data;
      try {
        const members = await store.leave(room, socket.id);
        joinedRooms.delete(room);
        await socket.leave(room);
        io.to(room).emit("presence", { room, members });
        ack({ ok: true });
      } catch (_error) {
        ack({ ok: false });
      }
    });

    socket.on("message:send", async (payload, ack) => {
      const parsed = sendSchema.safeParse(payload);
      if (!parsed.success) {
        ack({ ok: false, error: parsed.error.issues[0]?.message ?? "invalid payload" });
        return;
      }

      const { room, text, clientId } = parsed.data;
      try {
        const message = await store.append(room, {
          from: socket.data.name,
          text,
          clientId,
        });
        io.to(room).emit("message", message);
        ack({ ok: true, seq: message.seq });
      } catch (error) {
        ack({ ok: false, error: errorText(error, "send failed") });
      }
    });

    socket.on("typing", (payload) => {
      const parsed = typingSchema.safeParse(payload);
      if (!parsed.success) {
        return;
      }

      socket.to(parsed.data.room).emit("typing", {
        room: parsed.data.room,
        from: socket.data.name,
        on: parsed.data.on,
      });
    });

    socket.on("disconnect", () => {
      const rooms = [...joinedRooms];
      joinedRooms.clear();
      void (async () => {
        for (const room of rooms) {
          const members = await store.leave(room, socket.id);
          io.to(room).emit("presence", { room, members });
        }
      })().catch(() => undefined);
    });
  });

  try {
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        httpServer.off("listening", onListening);
        reject(error);
      };
      const onListening = () => {
        httpServer.off("error", onError);
        resolve();
      };
      httpServer.once("error", onError);
      httpServer.once("listening", onListening);
      httpServer.listen(opts.port ?? 3000);
    });
  } catch (error) {
    await redis.quit();
    throw error;
  }

  const address = httpServer.address();
  if (address === null || typeof address === "string") {
    await redis.quit();
    throw new Error("server did not start listening");
  }

  let closed = false;
  return {
    io,
    store,
    port: address.port,
    instanceId: opts.instanceId,
    async close() {
      if (closed) {
        return;
      }
      closed = true;
      await io.close();
      if (httpServer.listening) {
        await new Promise<void>((resolve) => httpServer.close(() => resolve()));
      }
      await redis.quit();
    },
  };
}
