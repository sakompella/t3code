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
  piStateIsIdle,
  type PiRpcConnection,
} from "./rpc.ts";
import { buildPiRpcLaunch, resolvePiLaunchArgs } from "./mcpInjection.ts";
import { detectPiFlavor } from "./status.ts";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";

const PI_TIMEOUT_MS = 180_000;

const isTextGenerationError = Schema.is(TextGenerationError);

const SETTLE_POLL_INTERVAL = Duration.millis(100);

/**
 * Waits until the one prompt this process was given is fully done. Pi says so
 * with `agent_settled`. Prime Agent has no such event, so after its first
 * `agent_end` this polls `get_state` until retries and post-run work finish.
 */
const waitForPromptSettled = (flavor: PiFlavor, connection: PiRpcConnection) =>
  Effect.gen(function* () {
    const terminalEvent = flavor.settleSignal === "agent_settled" ? "agent_settled" : "agent_end";
    while (true) {
      const event = yield* Queue.take(connection.events);
      if (event["type"] === terminalEvent) break;
    }
    if (flavor.settleSignal === "agent_settled") return;
    while (!piStateIsIdle(yield* connection.request({ type: "get_state" }))) {
      yield* Effect.sleep(SETTLE_POLL_INTERVAL);
    }
  });

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  flavor: PiFlavor,
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runPiJson: TextGenerationOperations.Runner = (request) => {
    const { operation, cwd, prompt, modelSelection } = request;
    return Effect.gen(function* () {
      const cliFlavor = yield* detectPiFlavor(flavor, piSettings, environment).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
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
        // `customModels` accepts arbitrary strings, so an unusable slug is
        // rejected rather than skipped: running Pi's default model here would
        // report success for a model the caller never asked for.
        const parsed = parsePiModelSlug(modelSelection.model);
        if (parsed === null) {
          return yield* new TextGenerationError({
            operation,
            detail: `${flavor.displayName} model '${modelSelection.model}' must use provider/model format.`,
          });
        }
        yield* connection.request({
          type: "set_model",
          provider: parsed.provider,
          modelId: parsed.modelId,
        });
      }

      yield* connection.request({ type: "prompt", message: prompt });
      yield* waitForPromptSettled(flavor, connection);
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
          detail: `${flavor.displayName} returned empty output.`,
        });
      }
      return yield* TextGenerationOperations.decodeJsonReply(request, flavor.displayName, text);
    }).pipe(
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
  };

  return TextGenerationOperations.fromRunner(
    "PiTextGeneration",
    TextGenerationOperations.withoutInvestigation("Pi", runPiJson),
  );
});
