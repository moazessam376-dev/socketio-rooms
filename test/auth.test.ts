import { afterEach, describe, expect, it } from "vitest";
import { cleanup, client, once, server, uniquePrefix } from "./helpers.js";

describe("handshake authentication", () => {
  afterEach(cleanup);

  it("rejects a bad name with connect_error", async () => {
    const running = await server({ prefix: uniquePrefix() });
    const socket = client(running.port, "bad name");

    const error = await once<Error>(socket, "connect_error");

    expect(error.message).toBe("invalid name");
  });

  it("connects a good name and sends server:hello", async () => {
    const running = await server({
      prefix: uniquePrefix(),
      instanceId: "auth-test-instance",
    });
    const socket = client(running.port, "alice");

    const hello = await once<{ instanceId: string }>(socket, "server:hello");

    expect(hello).toEqual({ instanceId: "auth-test-instance" });
  });
});
