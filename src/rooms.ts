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

function streamField(message: unknown, field: string): string {
  if (message instanceof Map) {
    const value = message.get(field);
    if (value !== undefined) {
      return String(value);
    }
  } else if (typeof message === "object" && message !== null) {
    const value = Reflect.get(message, field);
    if (value !== undefined) {
      return String(value);
    }
  }

  throw new Error(`stream message is missing ${field}`);
}

function messageFromStream(room: string, entry: { message: unknown }): Message {
  return {
    room,
    seq: Number(streamField(entry.message, "seq")),
    from: streamField(entry.message, "from"),
    text: streamField(entry.message, "text"),
    clientId: streamField(entry.message, "clientId"),
    ts: Number(streamField(entry.message, "ts")),
  };
}

export class RoomStore {
  private readonly redis: Redis;
  private readonly prefix: string;
  private readonly instanceId: string;
  private readonly bufferSize: number;
  private readonly clientIdTtlSeconds: number;
  private readonly instanceTtlSeconds: number;

  constructor(redis: Redis, opts: RoomStoreOptions) {
    this.redis = redis;
    this.prefix = opts.prefix;
    this.instanceId = opts.instanceId;
    this.bufferSize = opts.bufferSize ?? 500;
    this.clientIdTtlSeconds = opts.clientIdTtlSeconds ?? 300;
    this.instanceTtlSeconds = opts.instanceTtlSeconds ?? 10;
  }

  async join(room: string, member: Member): Promise<Member[]> {
    const roomKeys = keys(this.prefix).room(room);
    await this.redis.hSet(
      roomKeys.members,
      member.socketId,
      JSON.stringify({ name: member.name, instanceId: member.instanceId }),
    );
    return this.sweep(room);
  }

  async leave(room: string, socketId: string): Promise<Member[]> {
    const roomKeys = keys(this.prefix).room(room);
    await this.redis.hDel(roomKeys.members, socketId);
    return this.sweep(room);
  }

  async members(room: string): Promise<Member[]> {
    const storedMembers = await this.readMembers(room);
    const liveInstanceIds = await this.liveInstanceIds(storedMembers);
    return sortMembers(
      storedMembers.filter((member) => liveInstanceIds.has(member.instanceId)),
    );
  }

  async heartbeat(): Promise<void> {
    await this.redis.set(keys(this.prefix).instance(this.instanceId), "1", {
      EX: this.instanceTtlSeconds,
    });
  }

  async sweep(room: string): Promise<Member[]> {
    const roomKeys = keys(this.prefix).room(room);
    const storedMembers = await this.readMembers(room);
    const liveInstanceIds = await this.liveInstanceIds(storedMembers);
    const staleSocketIds = storedMembers
      .filter((member) => !liveInstanceIds.has(member.instanceId))
      .map((member) => member.socketId);

    if (staleSocketIds.length > 0) {
      await this.redis.hDel(roomKeys.members, staleSocketIds);
    }

    return sortMembers(
      storedMembers.filter((member) => liveInstanceIds.has(member.instanceId)),
    );
  }

  async recent(room: string, limit: number): Promise<Message[]> {
    const roomKeys = keys(this.prefix).room(room);
    const entries = await this.redis.xRevRange(roomKeys.stream, "+", "-", {
      COUNT: limit,
    });
    return entries.map((entry) => messageFromStream(room, entry)).reverse();
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

  async after(room: string, lastSeq: number): Promise<{ missed: Message[]; gap: boolean }> {
    const roomKeys = keys(this.prefix).room(room);
    const [entries, counter] = await Promise.all([
      this.redis.xRevRange(roomKeys.stream, "+", "-", { COUNT: this.bufferSize }),
      this.redis.get(roomKeys.seq),
    ]);

    if (entries.length === 0) {
      const currentSeq = Number(counter ?? 0);
      return { missed: [], gap: currentSeq > lastSeq };
    }

    const messages = entries
      .map((entry) => messageFromStream(room, entry))
      .reverse();
    return {
      missed: messages.filter((message) => message.seq > lastSeq),
      gap: messages[0].seq > lastSeq + 1,
    };
  }

  async clear(): Promise<void> {
    const matchingKeys: string[] = [];
    for await (const yieldedKeys of this.redis.scanIterator({
      MATCH: `${this.prefix}:*`,
      COUNT: 100,
    })) {
      const keys = Array.isArray(yieldedKeys) ? yieldedKeys : [yieldedKeys];
      matchingKeys.push(...keys.map((key) => String(key)));
    }

    for (let index = 0; index < matchingKeys.length; index += 100) {
      await this.redis.del(matchingKeys.slice(index, index + 100));
    }
  }

  private async readMembers(room: string): Promise<Member[]> {
    const roomKeys = keys(this.prefix).room(room);
    const values = await this.redis.hGetAll(roomKeys.members);
    return Object.entries(values).map(([socketId, value]) => {
      const member = JSON.parse(value) as Omit<Member, "socketId">;
      return { socketId, name: member.name, instanceId: member.instanceId };
    });
  }

  private async liveInstanceIds(members: Member[]): Promise<Set<string>> {
    const instanceIds = new Set(members.map((member) => member.instanceId));
    const checks = await Promise.all(
      [...instanceIds].map(async (instanceId) => {
        const exists = await this.redis.exists(keys(this.prefix).instance(instanceId));
        return exists > 0 ? instanceId : null;
      }),
    );
    return new Set(
      checks.filter((instanceId): instanceId is string => instanceId !== null),
    );
  }
}

function sortMembers(members: Member[]): Member[] {
  return members.sort((left, right) => left.socketId.localeCompare(right.socketId));
}
