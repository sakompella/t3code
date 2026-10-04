// @effect-diagnostics nodeBuiltinImport:off
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";

import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { HttpServerResponse } from "effect/unstable/http";

import { guardHttpResponseWriteErrors } from "./httpResponseErrorGuard.ts";
import { answerUnavailableUntilServing } from "./httpUnavailableUntilServing.ts";

// Bounds a failing run only: before the fix, these requests hang forever.
const CLIENT_TIMEOUT_MS = 5_000;

function getStatus(port: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const request = NodeHttp.get({ host: "127.0.0.1", port, path: "/" }, (response) => {
      response.resume();
      resolve(response.statusCode ?? 0);
    });
    request.on("error", reject);
    request.setTimeout(CLIENT_TIMEOUT_MS, () => reject(new Error("request timed out")));
  });
}

function upgradeStatusLine(port: number): Promise<string> {
  return new Promise((resolve, reject) => {
    const socket = NodeNet.connect(port, "127.0.0.1", () => {
      socket.write(
        [
          "GET /ws HTTP/1.1",
          "Host: 127.0.0.1",
          "Connection: Upgrade",
          "Upgrade: websocket",
          "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
          "Sec-WebSocket-Version: 13",
          "",
          "",
        ].join("\r\n"),
      );
    });
    socket.setTimeout(CLIENT_TIMEOUT_MS, () => {
      socket.destroy();
      reject(new Error("upgrade timed out"));
    });
    socket.once("data", (data) => {
      socket.destroy();
      resolve(data.toString().split("\r\n")[0] ?? "");
    });
    socket.on("error", reject);
  });
}

const listenWithoutServing = Effect.gen(function* () {
  const nodeServer = answerUnavailableUntilServing(
    guardHttpResponseWriteErrors(NodeHttp.createServer()),
  );
  const server = yield* NodeHttpServer.make(() => nodeServer, { host: "127.0.0.1", port: 0 });
  const { port } = nodeServer.address() as NodeNet.AddressInfo;
  return { server, port };
});

describe("answerUnavailableUntilServing", () => {
  it.live("answers 503 while listening before the app is served, then serves the app", () =>
    Effect.gen(function* () {
      const { server, port } = yield* listenWithoutServing;

      expect(yield* Effect.promise(() => getStatus(port))).toBe(503);

      yield* server.serve(Effect.succeed(HttpServerResponse.text("ok")));

      expect(yield* Effect.promise(() => getStatus(port))).toBe(200);
    }).pipe(Effect.scoped),
  );

  it.live("rejects websocket upgrades with 503 before the app is served", () =>
    Effect.gen(function* () {
      const { port } = yield* listenWithoutServing;

      expect(yield* Effect.promise(() => upgradeStatusLine(port))).toBe(
        "HTTP/1.1 503 Service Unavailable",
      );
    }).pipe(Effect.scoped),
  );
});
