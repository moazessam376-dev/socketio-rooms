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

type OutboxEntry = {
  room: string;
  text: string;
  clientId: string;
  attempts: number;
  notSent: boolean;
  nextAttemptId: number;
  inFlightAttemptId?: number;
  timer?: number;
};

function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (element === null) {
    throw new Error(`missing element ${selector}`);
  }
  return element;
}

const connectForm = required<HTMLFormElement>("#connect-form");
const nameInput = required<HTMLInputElement>("#name-input");
const connectButton = required<HTMLButtonElement>("#connect-button");
const roomApp = required<HTMLElement>("#room-app");
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

let socket: BrowserSocket | null = null;
const joinedRooms = new Set<string>();
const lastSeqByRoom = new Map<string, number>();
const seenSeqByRoom = new Map<string, Set<number>>();
const messagesByRoom = new Map<string, Message[]>();
const membersByRoom = new Map<string, Member[]>();
const typingByRoom = new Map<string, Set<string>>();
const truncatedRooms = new Set<string>();
const outbox = new Map<string, OutboxEntry>();

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

function retryOutboxEntry(clientId: string): void {
  const entry = outbox.get(clientId);
  if (entry === undefined || !entry.notSent) {
    return;
  }

  entry.attempts = 0;
  entry.notSent = false;
  sendError.textContent = "";
  renderMessages(entry.room);
  attemptSend(entry);
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

  for (const entry of outbox.values()) {
    if (entry.room !== room) {
      continue;
    }

    const item = document.createElement("li");
    const state = entry.notSent
      ? "not sent"
      : entry.inFlightAttemptId === undefined
        ? "waiting"
        : "sending";
    item.textContent = `[${state}] You: ${entry.text}`;
    if (entry.notSent) {
      const retryButton = document.createElement("button");
      retryButton.type = "button";
      retryButton.textContent = "Retry";
      retryButton.addEventListener("click", () => {
        retryOutboxEntry(entry.clientId);
      });
      item.append(" ", retryButton);
    }
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
  const activeSocket = socket;
  if (activeSocket === null) {
    return Promise.resolve({ ok: false, error: "not connected" });
  }

  const payload: { room: string; lastSeq?: number } = { room };
  if (requestedLastSeq !== undefined) {
    payload.lastSeq = requestedLastSeq;
  }

  return new Promise((resolve) => {
    activeSocket.emit("room:join", payload, (ack) => {
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

function cancelAttempt(entry: OutboxEntry): void {
  if (entry.timer !== undefined) {
    window.clearTimeout(entry.timer);
  }
  entry.timer = undefined;
  entry.inFlightAttemptId = undefined;
}

function sendFailure(entry: OutboxEntry, error: string): void {
  if (outbox.get(entry.clientId) !== entry || entry.notSent) {
    return;
  }

  entry.attempts += 1;
  sendError.textContent = error;
  if (entry.attempts >= 3) {
    entry.notSent = true;
  }
  if (currentRoom === entry.room) {
    renderMessages(entry.room);
  }

  if (!entry.notSent && socket?.connected === true) {
    attemptSend(entry);
  }
}

function attemptTimedOut(
  entry: OutboxEntry,
  activeSocket: BrowserSocket,
  attemptId: number,
): void {
  if (
    outbox.get(entry.clientId) !== entry ||
    entry.inFlightAttemptId !== attemptId
  ) {
    return;
  }

  entry.timer = undefined;
  entry.inFlightAttemptId = undefined;
  if (socket !== activeSocket || !activeSocket.connected) {
    return;
  }
  sendFailure(entry, "message acknowledgement timed out");
}

function attemptSend(entry: OutboxEntry): void {
  const activeSocket = socket;
  if (activeSocket === null || !activeSocket.connected || entry.notSent) {
    return;
  }

  cancelAttempt(entry);
  const attemptId = entry.nextAttemptId;
  entry.nextAttemptId += 1;
  entry.inFlightAttemptId = attemptId;
  entry.timer = window.setTimeout(() => {
    attemptTimedOut(entry, activeSocket, attemptId);
  }, 5000);
  if (currentRoom === entry.room) {
    renderMessages(entry.room);
  }

  activeSocket.emit(
    "message:send",
    {
      room: entry.room,
      text: entry.text,
      clientId: entry.clientId,
    },
    (ack: SendAck) => {
      if (
        outbox.get(entry.clientId) !== entry ||
        entry.inFlightAttemptId !== attemptId
      ) {
        return;
      }
      if (socket !== activeSocket || !activeSocket.connected) {
        cancelAttempt(entry);
        return;
      }

      cancelAttempt(entry);
      if (ack.ok) {
        outbox.delete(entry.clientId);
        if (currentRoom === entry.room) {
          renderMessages(entry.room);
        }
        sendError.textContent = "";
        return;
      }
      sendFailure(entry, ack.error);
    },
  );
}

function flushOutbox(): void {
  for (const entry of outbox.values()) {
    if (!entry.notSent) {
      attemptSend(entry);
    }
  }
}

function queueMessage(room: string, text: string): void {
  const entry: OutboxEntry = {
    room,
    text,
    clientId: crypto.randomUUID(),
    attempts: 0,
    notSent: false,
    nextAttemptId: 1,
  };
  outbox.set(entry.clientId, entry);
  renderMessages(room);
  attemptSend(entry);
}

function setTyping(on: boolean): void {
  if (socket !== null && currentRoom !== null) {
    socket.emit("typing", { room: currentRoom, on });
  }
}

function wireSocket(activeSocket: BrowserSocket): void {
  activeSocket.on("server:hello", ({ instanceId: nextInstanceId }) => {
    instanceId = nextInstanceId;
    renderStatus(statusLabel);
  });

  activeSocket.on("presence", ({ room, members }) => {
    membersByRoom.set(room, members);
    if (currentRoom === room) {
      renderMembers(room);
    }
  });

  activeSocket.on("message", rememberMessage);

  activeSocket.on("typing", ({ room, from, on }) => {
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

  activeSocket.on("connect", () => {
    const wasConnected = hasConnected;
    hasConnected = true;
    roomApp.hidden = false;
    flushOutbox();
    if (activeSocket.recovered) {
      renderStatus("recovered");
      return;
    }
    if (wasConnected) {
      void rejoinRooms();
      return;
    }
    renderStatus("connected");
  });

  activeSocket.on("disconnect", () => {
    for (const entry of outbox.values()) {
      cancelAttempt(entry);
    }
    if (currentRoom !== null) {
      renderMessages(currentRoom);
    }
    renderStatus("reconnecting", reconnectAttempts);
  });

  activeSocket.io.on("reconnect_attempt", (attempt) => {
    reconnectAttempts = attempt;
    renderStatus("reconnecting", attempt);
  });
}

connectForm.addEventListener("submit", (event) => {
  event.preventDefault();
  if (socket !== null) {
    return;
  }

  const name = nameInput.value.trim();
  if (name.length === 0) {
    return;
  }

  nameInput.disabled = true;
  connectButton.disabled = true;
  const activeSocket = io({ auth: { name }, autoConnect: false }) as BrowserSocket;
  socket = activeSocket;
  wireSocket(activeSocket);
  renderStatus("reconnecting", 0);
  activeSocket.connect();
});

joinForm.addEventListener("submit", (event) => {
  event.preventDefault();
  if (socket === null) {
    return;
  }
  const room = roomInput.value.trim();
  if (room.length > 0) {
    void joinRoom(room, lastSeqByRoom.get(room));
  }
});

messageForm.addEventListener("submit", (event) => {
  event.preventDefault();
  if (socket === null) {
    return;
  }
  const room = currentRoom;
  const text = messageInput.value.trim();
  if (room === null || text.length === 0) {
    return;
  }

  setTyping(false);
  messageInput.value = "";
  queueMessage(room, text);
});

messageInput.addEventListener("input", () => {
  setTyping(true);
  if (typingTimer !== undefined) {
    window.clearTimeout(typingTimer);
  }
  typingTimer = window.setTimeout(() => setTyping(false), 800);
});
