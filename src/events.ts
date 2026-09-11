import { z } from "zod";

export type Member = {
  socketId: string;
  name: string;
  instanceId: string;
};

export type Message = {
  room: string;
  seq: number;
  from: string;
  text: string;
  clientId: string;
  ts: number;
};

export type JoinAck =
  | { ok: true; members: Member[]; missed: Message[]; gap: boolean }
  | { ok: false; error: string };

export type SendAck = { ok: true; seq: number } | { ok: false; error: string };

export interface ClientToServerEvents {
  "room:join": (
    p: { room: string; lastSeq?: number },
    ack: (r: JoinAck) => void,
  ) => void;
  "room:leave": (p: { room: string }, ack: (r: { ok: boolean }) => void) => void;
  "message:send": (
    p: { room: string; text: string; clientId: string },
    ack: (r: SendAck) => void,
  ) => void;
  typing: (p: { room: string; on: boolean }) => void;
}

export interface ServerToClientEvents {
  "server:hello": (p: { instanceId: string }) => void;
  presence: (p: { room: string; members: Member[] }) => void;
  message: (m: Message) => void;
  typing: (p: { room: string; from: string; on: boolean }) => void;
}

export interface SocketData {
  name: string;
}

export const roomNameSchema: z.ZodString = z
  .string()
  .min(1)
  .max(32)
  .regex(/^[A-Za-z0-9_-]+$/);

export const joinSchema = z.object({
  room: roomNameSchema,
  lastSeq: z.number().int().nonnegative().optional(),
});

export const leaveSchema = z.object({
  room: roomNameSchema,
});

export const sendSchema = z.object({
  room: roomNameSchema,
  text: z.string().min(1).max(2000),
  clientId: z.string().min(1).max(64),
});

export const typingSchema = z.object({
  room: roomNameSchema,
  on: z.boolean(),
});
