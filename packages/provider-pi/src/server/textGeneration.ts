import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { TextGenerationError } from "@t3tools/contracts";
import type { PiSettings } from "../settings.ts";

import type { PiFlavor } from "./flavor.ts";
import {
  makePiRpcConnection,
  parsePiModelSlug,
  piRecordString,
  piStateIsIdle,
  type PiRpcConnection,
  type PiRpcRecord,
} from "./rpc.ts";
import { buildPiRpcLaunch, resolvePiLaunchArgs } from "./mcpInjection.ts";
import { detectPiFlavor } from "./status.ts";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";

const PI_TIMEOUT_MS = 180_000;

const isTextGenerationError = Schema.is(TextGenerationError);

// Pi's `errorMessage` is the provider's raw response text and can echo
// credentials, so only a leading HTTP status reaches the error detail.
const turnErrorDetail = (flavor: PiFlavor, errorMessage: string | undefined) => {
  const status = errorMessage?.match(/^([45]\d\d)\b/)?.[1];
  return status === undefined
    ? `${flavor.displayName} could not generate text: the model provider reported an error.`
    : `${flavor.displayName} could not generate text: the model provider returned HTTP ${status}.`;
};

const SETTLE_POLL_INTERVAL = Duration.millis(100);

/**
 * Waits until the prompt is fully done and returns its latest reply's error, if any. Pi
 * sends `agent_settled`. Prime Agent has none, so after its first `agent_end` this polls
 * `get_state` until retries and post-run work finish, still reading their replies.
 */
const waitForPromptSettled = Effect.fn("PiTextGeneration.waitForPromptSettled")(function* (
  flavor: PiFlavor,
  connection: PiRpcConnection,
) {
  const terminalEvent = flavor.settleSignal === "agent_settled" ? "agent_settled" : "agent_end";
  // Provider failures (bad key, rate limit, 5xx) arrive on the assistant
  // `message_end`. Keep only the latest, since Pi can retry and still answer.
  let turnError: string | undefined;
  const readReply = (event: PiRpcRecord) => {
    const message = event["message"];
    if (event["type"] === "message_end" && piRecordString(message, "role") === "assistant") {
      turnError =
        piRecordString(message, "stopReason") === "error"
          ? turnErrorDetail(flavor, piRecordString(message, "errorMessage"))
          : undefined;
    }
  };
  while (true) {
    const event = yield* Queue.take(connection.events);
    if (event["type"] === terminalEvent) break;
    readReply(event);
  }
  if (flavor.settleSignal === "agent_settled") return turnError;
  while (true) {
    const idle = piStateIsIdle(yield* connection.request({ type: "get_state" }));
    // Events sent before the `get_state` reply are queued by the time it arrives.
    for (const event of yield* Queue.clear(connection.events)) readReply(event);
    if (idle) return turnError;
    yield* Effect.sleep(SETTLE_POLL_INTERVAL);
  }
});

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  flavor: PiFlavor,
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runPiJson: TextGenerationOperations.Runner = Effect.fn("PiTextGeneration.runPiJson")(
    function* (request) {
      const { operation, cwd, prompt, modelSelection } = request;
      const cliFlavor = yield* detectPiFlavor(flavor, piSettings, environment).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError((error) => new TextGenerationError({ operation, detail: error.message })),
      );
      if (!cliFlavor.extensions) {
        return yield* new TextGenerationError({
          operation,
          detail: `${cliFlavor.displayName} cannot turn its tools off, and text generation never runs tools. Choose another text generation model in Settings.`,
        });
      }
      const resolvedLaunchArgs = resolvePiLaunchArgs(piSettings.launchArgs);
      if (!resolvedLaunchArgs.ok) {
        return yield* new TextGenerationError({
          operation,
          detail: resolvedLaunchArgs.message,
        });
      }
      const launch = buildPiRpcLaunch({
        launchArgs: resolvedLaunchArgs.args,
        environment,
        mcpSession: undefined,
        extensionPath: undefined,
        ephemeral: true,
        disableExtensions: true,
        // Background naming/content helpers must never mutate the workspace.
        disableTools: true,
      });
      const connection = yield* makePiRpcConnection({
        command: piSettings.binaryPath || flavor.defaultBinary,
        args: launch.args,
        cwd,
        env: launch.env,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      if (modelSelection.model !== "default") {
        // Reject unusable custom slugs; silently using Pi's default would misreport the requested model.
        const parsed = parsePiModelSlug(modelSelection.model);
        if (parsed === null) {
          return yield* new TextGenerationError({
            operation,
            detail: `${flavor.displayName} model '${modelSelection.model}' must use provider/model format.`,
          });
        }
        yield* connection
          .request({ type: "set_model", provider: parsed.provider, modelId: parsed.modelId })
          .pipe(
            // Model discovery loads extensions that this background helper disables.
            Effect.mapError((error) =>
              error._tag === "PiRpcError" && error.operation === "set_model"
                ? new TextGenerationError({
                    operation,
                    detail:
                      `${flavor.displayName} rejected model '${modelSelection.model}': ${error.detail ?? "unknown error"}. ` +
                      `If the model comes from a ${flavor.displayName} extension or package, text generation cannot use it ` +
                      `because it runs ${flavor.displayName} with --no-extensions. Pick a built-in model or one defined ` +
                      `in ${flavor.displayName}'s models.json.`,
                    cause: error,
                  })
                : error,
            ),
          );
      }

      yield* connection.request({ type: "prompt", message: prompt });
      const turnError = yield* waitForPromptSettled(flavor, connection);
      const data = yield* connection.request({ type: "get_last_assistant_text" });
      const text =
        typeof data === "object" &&
        data !== null &&
        typeof (data as { text?: unknown }).text === "string"
          ? (data as { text: string }).text.trim()
          : "";
      if (!text) {
        return yield* new TextGenerationError({
          operation,
          detail: turnError ?? `${flavor.displayName} returned empty output.`,
        });
      }
      return yield* TextGenerationOperations.decodeJsonReply(request, flavor.displayName, text);
    },
    (effect, request) => {
      const { operation } = request;
      return effect.pipe(
        Effect.timeoutOption(PI_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new TextGenerationError({
                  operation,
                  detail: `${flavor.displayName} request timed out.`,
                }),
              ),
            onSome: (value) => Effect.succeed(value),
          }),
        ),
        Effect.mapError((cause) =>
          isTextGenerationError(cause)
            ? cause
            : new TextGenerationError({
                operation,
                detail: `${flavor.displayName} text generation failed.`,
                cause,
              }),
        ),
        Effect.scoped,
      );
    },
  );

  return TextGenerationOperations.fromRunner(
    "PiTextGeneration",
    TextGenerationOperations.withoutInvestigation("Pi", runPiJson),
  );
});
