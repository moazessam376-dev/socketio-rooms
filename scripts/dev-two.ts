import { spawn, type ChildProcess } from "node:child_process";

const children: ChildProcess[] = [
  spawn(process.execPath, ["--import", "tsx", "src/main.ts"], {
    env: { ...process.env, PORT: "3001", INSTANCE_ID: "a", ADAPTER: "1" },
    stdio: "inherit",
  }),
  spawn(process.execPath, ["--import", "tsx", "src/main.ts"], {
    env: { ...process.env, PORT: "3002", INSTANCE_ID: "b", ADAPTER: "1" },
    stdio: "inherit",
  }),
];

let stopping = false;

function stop(signal: NodeJS.Signals): void {
  if (stopping) {
    return;
  }
  stopping = true;
  for (const child of children) {
    if (!child.killed) {
      child.kill(signal);
    }
  }
  process.exit(0);
}

process.once("SIGINT", () => stop("SIGINT"));
process.once("SIGTERM", () => stop("SIGTERM"));

for (const child of children) {
  child.once("error", () => {
    if (!stopping) {
      stop("SIGTERM");
    }
  });
  child.once("exit", (code) => {
    if (!stopping) {
      stopping = true;
      for (const other of children) {
        if (other !== child && !other.killed) {
          other.kill("SIGTERM");
        }
      }
      process.exit(code ?? 1);
    }
  });
}
