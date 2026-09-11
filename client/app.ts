import type { Socket } from "socket.io-client";
import type {
  ClientToServerEvents,
  JoinAck,
  Member,
  Message,
  SendAck,
  ServerToClientEvents,
} from "../src/events.js";

declare const io: typeof import("socket.io-client").io;

type BrowserSocket = Socket<ServerToClientEvents, ClientToServerEvents>;
type StatusLabel =
  | "connected"
  | "recovered"
  | "rejoined"
  | "reconnecting";

function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (element === null) {
    throw new Error(`missing element ${selector}`);
  }
  return element;
}

const joinForm = required<HTMLFormElement>("#join-form");
const roomInput = required<HTMLInputElement>("#room-input");
const membersList = required<HTMLUListElement>("#members");
const roomTitle = required<HTMLElement>("#room-title");
const messagesList = required<HTMLOListElement>("#messages");
const historyNotice = required<HTMLElement>("#history-notice");
const typingLine = required<HTMLElement>("#typing-line");
const statusLine = required<HTMLElement>("#status");
const messageForm = required<HTMLFormElement>("#message-form");
const messageInput = required<HTMLInputElement>("#message-input");
const sendError = required<HTMLElement>("#send-error");

const promptedName = window.prompt("Name")?.trim() ?? "";
const socket = io({ auth: { name: promptedName } }) as BrowserSocket;
const joinedRooms = new Set<string>();
const lastSeqByRoom = new Map<string, number>();
const seenSeqByRoom = new Map<string, Set<number>>();
const messagesByRoom = new Map<string, Message[]>();
const membersByRoom = new Map<string, Member[]>();
const typingByRoom = new Map<string, Set<string>>();
const truncatedRooms = new Set<string>();

let currentRoom: string | null = null;
let instanceId = "unknown";
let reconnectAttempts = 0;
let hasConnected = false;
let typingTimer: number | undefined;
let statusLabel: StatusLabel = "reconnecting";

function renderStatus(label: StatusLabel, attempt = reconnectAttempts): void {
  statusLabel = label;
  const status =
    label === "reconnecting" ? `reconnecting (attempt ${attempt})` : label;
  statusLine.textContent =
    `${status} | instance ${instanceId} | reconnect attempts ${reconnectAttempts}`;
}

function renderMembers(room: string): void {
  membersList.replaceChildren();
  for (const member of membersByRoom.get(room) ?? []) {
    const item = document.createElement("li");
    item.textContent = `${member.name} (${member.instanceId})`;
    membersList.append(item);
  }
}

function renderMessages(room: string): void {
  messagesList.replaceChildren();
  const messages = [...(messagesByRoom.get(room) ?? [])].sort(
    (left, right) => left.seq - right.seq,
  );
  for (const message of messages) {
    const item = document.createElement("li");
    item.textContent = `[${message.seq}] ${message.from}: ${message.text}`;
    messagesList.append(item);
  }
}

function renderTyping(room: string): void {
  const names = [...(typingByRoom.get(room) ?? [])];
  typingLine.textContent = names.length > 0 ? `${names.join(", ")} is typing` : "";
}

function renderRoom(room: string): void {
  currentRoom = room;
  roomTitle.textContent = `Room: ${room}`;
  renderMembers(room);
  renderMessages(room);
  renderTyping(room);
  historyNotice.hidden = !truncatedRooms.has(room);
}

function rememberMessage(message: Message): void {
  const seen = seenSeqByRoom.get(message.room) ?? new Set<number>();
  if (seen.has(message.seq)) {
    return;
  }
  seen.add(message.seq);
  seenSeqByRoom.set(message.room, seen);

  const messages = messagesByRoom.get(message.room) ?? [];
  messages.push(message);
  messagesByRoom.set(message.room, messages);
  const previousLastSeq = lastSeqByRoom.get(message.room) ?? 0;
  if (message.seq > previousLastSeq) {
    lastSeqByRoom.set(message.room, message.seq);
  }

  if (currentRoom === message.room) {
    renderMessages(message.room);
  }
}

function joinRoom(room: string, requestedLastSeq?: number): Promise<JoinAck> {
  const payload: { room: string; lastSeq?: number } = { room };
  if (requestedLastSeq !== undefined) {
    payload.lastSeq = requestedLastSeq;
  }

  return new Promise((resolve) => {
    socket.emit("room:join", payload, (ack) => {
      if (!ack.ok) {
        sendError.textContent = ack.error;
        resolve(ack);
        return;
      }

      joinedRooms.add(room);
      if (!lastSeqByRoom.has(room)) {
        lastSeqByRoom.set(room, 0);
      }
      membersByRoom.set(room, ack.members);
      for (const message of ack.missed) {
        rememberMessage(message);
      }
      if (ack.gap) {
        truncatedRooms.add(room);
      }
      renderRoom(room);
      sendError.textContent = "";
      resolve(ack);
    });
  });
}

async function rejoinRooms(): Promise<void> {
  const roomBeforeReconnect = currentRoom;
  const rooms = [...joinedRooms];
  const results = await Promise.all(
    rooms.map((room) => joinRoom(room, lastSeqByRoom.get(room) ?? 0)),
  );
  if (roomBeforeReconnect !== null && joinedRooms.has(roomBeforeReconnect)) {
    renderRoom(roomBeforeReconnect);
  }
  if (results.every((result) => result.ok)) {
    renderStatus("rejoined");
  }
}

function sendWithRetries(room: string, text: string): Promise<SendAck> {
  const payload = {
    room,
    text,
    clientId: crypto.randomUUID(),
  };
  const maxRetries = 3;

  return new Promise((resolve) => {
    let retries = 0;

    const attempt = (): void => {
      socket.timeout(5000).emit("message:send", payload, (error, ack) => {
        if (error) {
          if (retries < maxRetries) {
            retries += 1;
            attempt();
            return;
          }
          resolve({ ok: false, error: "message acknowledgement timed out" });
          return;
        }
        resolve(ack);
      });
    };

    attempt();
  });
}

function setTyping(on: boolean): void {
  if (currentRoom !== null) {
    socket.emit("typing", { room: currentRoom, on });
  }
}

joinForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const room = roomInput.value.trim();
  if (room.length > 0) {
    void joinRoom(room, lastSeqByRoom.get(room));
  }
});

messageForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const room = currentRoom;
  const text = messageInput.value.trim();
  if (room === null || text.length === 0) {
    return;
  }

  setTyping(false);
  messageInput.value = "";
  void sendWithRetries(room, text).then((ack) => {
    if (!ack.ok) {
      sendError.textContent = ack.error;
    }
  });
});

messageInput.addEventListener("input", () => {
  setTyping(true);
  if (typingTimer !== undefined) {
    window.clearTimeout(typingTimer);
  }
  typingTimer = window.setTimeout(() => setTyping(false), 800);
});

socket.on("server:hello", ({ instanceId: nextInstanceId }) => {
  instanceId = nextInstanceId;
  renderStatus(statusLabel);
});

socket.on("presence", ({ room, members }) => {
  membersByRoom.set(room, members);
  if (currentRoom === room) {
    renderMembers(room);
  }
});

socket.on("message", rememberMessage);

socket.on("typing", ({ room, from, on }) => {
  const names = typingByRoom.get(room) ?? new Set<string>();
  if (on) {
    names.add(from);
  } else {
    names.delete(from);
  }
  typingByRoom.set(room, names);
  if (currentRoom === room) {
    renderTyping(room);
  }
});

socket.on("connect", () => {
  const wasConnected = hasConnected;
  hasConnected = true;
  if (socket.recovered) {
    renderStatus("recovered");
    return;
  }
  if (wasConnected) {
    void rejoinRooms();
    return;
  }
  renderStatus("connected");
});

socket.on("disconnect", () => {
  renderStatus("reconnecting", reconnectAttempts);
});

socket.io.on("reconnect_attempt", (attempt) => {
  reconnectAttempts = attempt;
  renderStatus("reconnecting", attempt);
});

renderStatus("reconnecting", 0);
