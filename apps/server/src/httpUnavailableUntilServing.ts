// @effect-diagnostics nodeBuiltinImport:off
import type * as NodeHttp from "node:http";
import type * as NodeStream from "node:stream";

const RETRY_AFTER_SECONDS = "1";

/**
 * Effect's NodeHttpServer starts listening when its layer is built, but only
 * attaches the app's "request" and "upgrade" handlers once every route layer
 * is built, which takes seconds during startup. A request in that gap reaches
 * no handler and hangs until the client gives up; the desktop readiness probe
 * lost a full probe timeout to this on every launch.
 *
 * Until the app attaches its own handler, answer 503 with Retry-After so
 * clients retry promptly instead of hanging. Apply this after any other
 * listeners that are not the app, since the next listener ends the 503s.
 */
export function answerUnavailableUntilServing<T extends NodeHttp.Server>(server: T): T {
  const answerRequest = (_request: NodeHttp.IncomingMessage, response: NodeHttp.ServerResponse) => {
    response.writeHead(503, { "retry-after": RETRY_AFTER_SECONDS, connection: "close" });
    response.end();
  };
  const answerUpgrade = (_request: NodeHttp.IncomingMessage, socket: NodeStream.Duplex) => {
    socket.end(
      `HTTP/1.1 503 Service Unavailable\r\nRetry-After: ${RETRY_AFTER_SECONDS}\r\nConnection: close\r\n\r\n`,
    );
  };

  server.on("request", answerRequest);
  server.on("upgrade", answerUpgrade);
  server.on("newListener", (event, listener) => {
    if (event === "request" && listener !== answerRequest) server.off("request", answerRequest);
    if (event === "upgrade" && listener !== answerUpgrade) server.off("upgrade", answerUpgrade);
  });
  return server;
}
