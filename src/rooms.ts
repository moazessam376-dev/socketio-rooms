import type { Member, Message } from "./events.js";
import { keys, type Redis } from "./redis.js";

export type RoomStoreOptions = {
  prefix: string;
  instanceId: string;
  bufferSize?: number;
  clientIdTtlSeconds?: number;
  instanceTtlSeconds?: number;
};

const APPEND_SCRIPT = `
local existing = redis.call('GET', KEYS[1])
if existing then
  return existing
end

local seq = redis.call('INCR', KEYS[2])
local ts = tonumber(ARGV[5])
redis.call(
  'XADD', KEYS[3], 'MAXLEN', '~', ARGV[1], '*',
  'seq', tostring(seq),
  'from', ARGV[2],
  'text', ARGV[3],
  'clientId', ARGV[4],
  'ts', tostring(ts)
)

local message = cjson.encode({
  room = ARGV[7],
  seq = seq,
  from = ARGV[2],
  text = ARGV[3],
  clientId = ARGV[4],
  ts = ts
})
redis.call('SET', KEYS[1], message, 'NX', 'EX', ARGV[6])
return message
`;

export class RoomStore {
  private readonly redis: Redis;
  private readonly prefix: string;
  private readonly bufferSize: number;
  private readonly clientIdTtlSeconds: number;

  constructor(redis: Redis, opts: RoomStoreOptions) {
    this.redis = redis;
    this.prefix = opts.prefix;
    this.bufferSize = opts.bufferSize ?? 500;
    this.clientIdTtlSeconds = opts.clientIdTtlSeconds ?? 300;
  }

  async join(room: string, member: Member): Promise<Member[]> {
    const roomKeys = keys(this.prefix).room(room);
    await this.redis.hSet(
      roomKeys.members,
      member.socketId,
      JSON.stringify({ name: member.name, instanceId: member.instanceId }),
    );
    return this.members(room);
  }

  async leave(room: string, socketId: string): Promise<Member[]> {
    const roomKeys = keys(this.prefix).room(room);
    await this.redis.hDel(roomKeys.members, socketId);
    return this.members(room);
  }

  async members(room: string): Promise<Member[]> {
    const roomKeys = keys(this.prefix).room(room);
    const values = await this.redis.hGetAll(roomKeys.members);
    const result = Object.entries(values).map(([socketId, value]) => {
      const member = JSON.parse(value) as Omit<Member, "socketId">;
      return { socketId, name: member.name, instanceId: member.instanceId };
    });
    return result.sort((left, right) => left.socketId.localeCompare(right.socketId));
  }

  async append(
    room: string,
    input: { from: string; text: string; clientId: string },
  ): Promise<Message> {
    const roomKeys = keys(this.prefix).room(room);
    const clientKey = keys(this.prefix).clientId(room, input.clientId);
    const result = await this.redis.eval(APPEND_SCRIPT, {
      keys: [clientKey, roomKeys.seq, roomKeys.stream],
      arguments: [
        String(this.bufferSize),
        input.from,
        input.text,
        input.clientId,
        String(Date.now()),
        String(this.clientIdTtlSeconds),
        room,
      ],
    });

    if (typeof result !== "string") {
      throw new Error("append script returned an invalid message");
    }

    return JSON.parse(result) as Message;
  }

  async after(_room: string, _lastSeq: number): Promise<{ missed: Message[]; gap: boolean }> {
    throw new Error("not implemented");
  }

  async clear(): Promise<void> {
    const matchingKeys: string[] = [];
    for await (const key of this.redis.scanIterator({
      MATCH: `${this.prefix}:*`,
      COUNT: 100,
    })) {
      matchingKeys.push(String(key));
    }

    if (matchingKeys.length > 0) {
      await this.redis.del(matchingKeys);
    }
  }
}
