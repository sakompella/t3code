import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { PiRpcError, PiRpcTimeoutError, type PiRpcConnection, type PiRpcRecord } from "./PiRpc.ts";
import { materializeExtensionSource } from "./piT3McpInjection.ts";
import {
  PRIME_AGENT_DIALOG_COMMAND,
  PRIME_AGENT_DIALOG_EXTENSION_FILENAME,
  PRIME_AGENT_DIALOG_EXTENSION_SOURCE,
  PRIME_AGENT_DIALOG_RESULT,
} from "./primeAgentDialogExtensionSource.ts";

const Result = Schema.Struct({
  requestId: Schema.String,
  ok: Schema.Boolean,
  error: Schema.optional(Schema.String),
});
const decodeResult = Schema.decodeUnknownOption(Schema.fromJsonString(Result));
const decodeCommands = Schema.decodeUnknownOption(
  Schema.Struct({
    commands: Schema.Array(Schema.Struct({ name: Schema.String })),
  }),
);
const encodeRoute = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      requestId: Schema.String,
      activeSessionId: Schema.String,
      socketPath: Schema.optional(Schema.String),
    }),
  ),
);

export const materializePrimeAgentDialogExtension = Effect.fn(
  "materializePrimeAgentDialogExtension",
)(function* (cacheDir: string) {
  return yield* materializeExtensionSource(
    cacheDir,
    PRIME_AGENT_DIALOG_EXTENSION_FILENAME,
    PRIME_AGENT_DIALOG_EXTENSION_SOURCE,
  );
});

/**
 * The daemon socket Prime Agent uses, from `resolvePiLaunchArgs` output (equals forms already split).
 * Like Prime Agent's own parser, the last `--daemon-socket` wins.
 */
export function daemonSocketFromLaunchArgs(args: ReadonlyArray<string>): string | undefined {
  const index = args.lastIndexOf("--daemon-socket");
  return index < 0 ? undefined : args[index + 1];
}

export function makePrimeAgentChildDialogs() {
  const pending = new Map<string, Deferred.Deferred<typeof Result.Type>>();
  let counter = 0;
  const consumeEvent = Effect.fnUntraced(function* (event: PiRpcRecord) {
    const message = event["message"];
    if (
      event["type"] !== "extension_ui_request" ||
      event["method"] !== "notify" ||
      typeof message !== "string" ||
      !message.startsWith(PRIME_AGENT_DIALOG_RESULT)
    )
      return false;
    const result = decodeResult(message.slice(PRIME_AGENT_DIALOG_RESULT.length));
    if (result._tag === "Some") {
      const waiter = pending.get(result.value.requestId);
      if (waiter !== undefined) yield* Deferred.succeed(waiter, result.value);
    }
    return true;
  });
  const attach = Effect.fnUntraced(function* (
    connection: PiRpcConnection,
    activeSessionId: string,
    socketPath: string | undefined,
  ) {
    const commands = yield* connection.request({ type: "get_commands" });
    const available = decodeCommands(commands);
    if (
      available._tag === "None" ||
      !available.value.commands.some((command) => command.name === PRIME_AGENT_DIALOG_COMMAND)
    )
      return yield* new PiRpcError({
        operation: "child-dialog-routing",
        detail:
          "The T3 Prime Agent dialog extension did not load; child observation was not enabled.",
      });
    const requestId = `child-dialog-${counter++}`;
    const waiter = yield* Deferred.make<typeof Result.Type>();
    pending.set(requestId, waiter);
    const setup = Effect.gen(function* () {
      yield* connection.request({
        type: "prompt",
        message: `/${PRIME_AGENT_DIALOG_COMMAND} ${encodeRoute({ requestId, activeSessionId, ...(socketPath === undefined ? {} : { socketPath }) })}`,
      });
      // The RPC prompt response acknowledges admission, not command completion.
      const result = yield* Effect.raceFirst(
        Deferred.await(waiter),
        connection.exited.pipe(
          Effect.andThen(
            new PiRpcError({
              operation: "child-dialog-routing",
              detail: "Prime Agent exited before routing was ready.",
            }),
          ),
        ),
      ).pipe(
        Effect.timeoutOrElse({
          duration: Duration.seconds(15),
          orElse: () =>
            new PiRpcTimeoutError({ operation: "child-dialog-routing", timeoutMs: 15_000 }),
        }),
      );
      if (!result.ok)
        return yield* new PiRpcError({
          operation: "child-dialog-routing",
          detail: result.error ?? "Prime Agent could not attach a child dialog recipient.",
        });
    });
    yield* setup.pipe(Effect.ensuring(Effect.sync(() => pending.delete(requestId))));
  });
  return { consumeEvent, attach };
}
