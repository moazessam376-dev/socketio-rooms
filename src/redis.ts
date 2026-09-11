import { createClient } from "redis";

export type Redis = ReturnType<typeof createClient>;

export async function connectRedis(url: string): Promise<Redis> {
  const redis = createClient({ url });
  await redis.connect();
  return redis as Redis;
}

export function keys(prefix: string): {
  instance(id: string): string;
  room(room: string): { seq: string; stream: string; members: string };
  clientId(room: string, clientId: string): string;
} {
  return {
    instance: (id) => `${prefix}:instance:${id}`,
    room: (room) => ({
      seq: `${prefix}:room:${room}:seq`,
      stream: `${prefix}:room:${room}:stream`,
      members: `${prefix}:room:${room}:members`,
    }),
    clientId: (room, clientId) => `${prefix}:room:${room}:client:${clientId}`,
  };
}
