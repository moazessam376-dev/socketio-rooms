import { startServer } from "./server.js";

const port = Number(process.env.PORT ?? 3000);
const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const instanceId = process.env.INSTANCE_ID ?? `instance-${process.pid}`;

const running = await startServer({
  port,
  redisUrl,
  instanceId,
});

const shutdown = async () => {
  await running.close();
  process.exit(0);
};

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
