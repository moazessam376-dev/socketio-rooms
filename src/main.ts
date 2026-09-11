import { startServer } from "./server.js";

const port = Number(process.env.PORT ?? 3000);
const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const prefix = process.env.PREFIX ?? "socketio-rooms";
const instanceId = process.env.INSTANCE_ID ?? `instance-${process.pid}`;

const running = await startServer({
  port,
  redisUrl,
  prefix,
  instanceId,
  adapter: process.env.ADAPTER === "1",
  serveStatic: true,
});

console.log(`listening on ${running.port} as ${running.instanceId}`);

const shutdown = async () => {
  await running.close();
  process.exit(0);
};

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
