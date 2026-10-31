import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { makePiTextGeneration } from "./textGeneration.ts";
import { PI_FLAVOR, PRIME_AGENT_FLAVOR } from "./flavor.ts";
import {
  PRIME_AGENT_098_CAPABILITIES,
  PRIME_AGENT_098_HELP,
  PRIME_AGENT_FORK_CAPABILITIES,
  PRIME_AGENT_FORK_HELP,
} from "./primeAgentHelp.testkit.ts";

const decodeJsonLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const makeFakePi = (
  reply: string,
  setModelError?: string,
  turnEvents: ReadonlyArray<Record<string, unknown>> = [],
) =>
  Effect.gen(function* () {
    const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
    const received: Array<Record<string, unknown>> = [];
    const emit = (record: Record<string, unknown>) =>
      Queue.offer(stdout, new TextEncoder().encode(`${encodeJsonLine(record)}\n`));
    let buffered = "";
    const onStdin = (chunk: Uint8Array) =>
      Effect.gen(function* () {
        buffered += new TextDecoder().decode(chunk);
        const lines = buffered.split("\n");
        buffered = lines.pop() ?? "";
        for (const line of lines.filter((candidate) => candidate.length > 0)) {
          const record = decodeJsonLine(line) as Record<string, unknown>;
          received.push(record);
          const response = { type: "response", id: record["id"], command: record["type"] };
          if (record["type"] === "prompt") {
            yield* emit({ ...response, success: true });
            for (const event of turnEvents) yield* emit(event);
            yield* emit({ type: "agent_settled" });
          } else if (record["type"] === "get_last_assistant_text") {
            yield* emit({ ...response, success: true, data: { text: reply } });
          } else if (record["type"] === "set_model" && setModelError !== undefined) {
            yield* emit({ ...response, success: false, error: setModelError });
          } else {
            yield* emit({ ...response, success: true });
          }
        }
      });
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          // Outside the valid pid range, so PiRpc's process-group kill never lands.
          pid: ChildProcessSpawner.ProcessId(999_999_999),
          exitCode: Effect.never,
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.forEach(onStdin),
          stdout: Stream.fromQueue(stdout),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      ),
    );
    return { spawner, received };
  });

it.effect("names the model and the disabled extensions when Pi cannot load it", () =>
  Effect.gen(function* () {
    const pi = yield* makeFakePi("", "Model not found: demo/demo-model");
    const textGeneration = yield* makePiTextGeneration(
      PI_FLAVOR,
      { enabled: true, binaryPath: "pi", launchArgs: "", customModels: [] },
      {},
    ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, pi.spawner));

    const error = yield* textGeneration
      .generateCommitMessage({
        cwd: process.cwd(),
        branch: "main",
        stagedSummary: "M README.md",
        stagedPatch: "diff --git a/README.md b/README.md",
        modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "demo/demo-model"),
      })
      .pipe(Effect.flip);

    assert.equal(error._tag, "TextGenerationError");
    assert.include(error.detail, "Pi rejected model 'demo/demo-model'");
    assert.include(error.detail, "Model not found: demo/demo-model");
    assert.include(error.detail, "--no-extensions");
    assert.isUndefined(pi.received.find((record) => record["type"] === "prompt"));
  }),
);

const failedTurnWith = (errorMessage: string) => ({
  type: "message_end",
  message: { role: "assistant", stopReason: "error", errorMessage },
});

const failedTurn = failedTurnWith(
  '401: {"error":{"message":"Incorrect API key provided: sk-test-SECRET123"}}',
);

const generateCommitMessage = (turnEvents: ReadonlyArray<Record<string, unknown>>, reply = "") =>
  Effect.gen(function* () {
    const pi = yield* makeFakePi(reply, undefined, turnEvents);
    const textGeneration = yield* makePiTextGeneration(
      PI_FLAVOR,
      { enabled: true, binaryPath: "pi", launchArgs: "", customModels: [] },
      {},
    ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, pi.spawner));
    return yield* textGeneration.generateCommitMessage({
      cwd: process.cwd(),
      branch: "main",
      stagedSummary: "M README.md",
      stagedPatch: "diff --git a/README.md b/README.md",
      modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "default"),
    });
  });

it.effect("reports Pi's turn error when the reply is empty", () =>
  Effect.gen(function* () {
    const error = yield* generateCommitMessage([failedTurn]).pipe(Effect.flip);

    assert.equal(error._tag, "TextGenerationError");
    assert.equal(error.detail, "Pi could not generate text: the model provider returned HTTP 401.");
  }),
);

it.effect("keeps Pi's raw error text out of the error", () =>
  Effect.gen(function* () {
    const error = yield* generateCommitMessage([
      failedTurnWith("Invalid API key sk-test-SECRET123"),
    ]).pipe(Effect.flip);

    assert.equal(error.detail, "Pi could not generate text: the model provider reported an error.");
    assert.notInclude(error.message, "SECRET123");
  }),
);

it.effect("keeps the reply when Pi answers after a failed attempt", () =>
  Effect.gen(function* () {
    const result = yield* generateCommitMessage(
      [failedTurn, { type: "message_end", message: { role: "assistant", stopReason: "stop" } }],
      JSON.stringify({ subject: "Update README", body: "" }),
    );

    assert.equal(result.subject, "Update README");
  }),
);

it.effect("drops an earlier error once a later attempt succeeds", () =>
  Effect.gen(function* () {
    const error = yield* generateCommitMessage([
      failedTurn,
      { type: "message_end", message: { role: "assistant", stopReason: "stop" } },
    ]).pipe(Effect.flip);

    assert.equal(error.detail, "Pi returned empty output.");
  }),
);

/**
 * `pi` behind a `prime-agent` binary that answers `--version`, `--help` and
 * `--capabilities` as given.
 */
const primeAgentCli = (
  pi: { readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"] },
  cli: {
    readonly version: string;
    readonly help: string;
    readonly helpExitCode: number;
    readonly capabilities: string;
  },
) => {
  const spawned: Array<ReadonlyArray<string>> = [];
  const output = (text: string, exitCode: number) =>
    ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(999_999_999),
      exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(exitCode)),
      isRunning: Effect.succeed(false),
      kill: () => Effect.void,
      unref: Effect.succeed(Effect.void),
      stdin: Sink.drain,
      stdout: Stream.succeed(new TextEncoder().encode(text)),
      stderr: Stream.empty,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    });
  const spawner = ChildProcessSpawner.make((command) => {
    const args = ChildProcess.isStandardCommand(command) ? command.args : [];
    if (args.includes("--version")) return Effect.succeed(output(`${cli.version}\n`, 0));
    if (args.includes("--help")) return Effect.succeed(output(cli.help, cli.helpExitCode));
    if (args.includes("--capabilities")) return Effect.succeed(output(cli.capabilities, 0));
    spawned.push(args);
    return pi.spawner.spawn(command);
  });
  return { spawner, rpcLaunches: () => spawned };
};

it.effect.each([
  {
    name: "the 0.10 fork, which cannot turn its tools off",
    cli: {
      version: "0.10.0",
      help: PRIME_AGENT_FORK_HELP,
      helpExitCode: 0,
      capabilities: PRIME_AGENT_FORK_CAPABILITIES,
    },
    detail: "tools",
  },
  {
    name: "a binary whose flavor T3 cannot tell",
    cli: {
      version: "0.9.8",
      help: "",
      helpExitCode: 1,
      capabilities: PRIME_AGENT_098_CAPABILITIES.stdout,
    },
    detail: "prime-agent",
  },
])("refuses Prime Agent text generation on $name", ({ cli, detail }) =>
  Effect.gen(function* () {
    const pi = yield* makeFakePi(encodeJsonLine({ title: "Unused" }));
    const binary = primeAgentCli(pi, cli);
    const textGeneration = yield* makePiTextGeneration(
      PRIME_AGENT_FLAVOR,
      { enabled: true, binaryPath: "prime-agent", launchArgs: "", customModels: [] },
      {},
    ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, binary.spawner));

    const error = yield* textGeneration
      .generateThreadTitle({
        cwd: process.cwd(),
        message: "Rename the settings page",
        modelSelection: createModelSelection(ProviderInstanceId.make("primeAgent"), "default"),
      })
      .pipe(Effect.flip);

    assert.include(error.detail, detail);
    assert.deepEqual(binary.rpcLaunches(), []);
    assert.deepEqual(pi.received, []);
  }),
);

it.effect("reports Prime Agent's last retry error, which arrives after its first agent_end", () =>
  Effect.gen(function* () {
    const pi = yield* makeFakePi("", undefined, [
      failedTurnWith("500 Internal Server Error"),
      { type: "agent_end" },
      failedTurn,
    ]);
    const binary = primeAgentCli(pi, {
      version: "0.9.8",
      help: PRIME_AGENT_098_HELP,
      helpExitCode: 0,
      capabilities: PRIME_AGENT_098_CAPABILITIES.stdout,
    });
    const textGeneration = yield* makePiTextGeneration(
      PRIME_AGENT_FLAVOR,
      { enabled: true, binaryPath: "prime-agent", launchArgs: "", customModels: [] },
      {},
    ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, binary.spawner));

    const error = yield* textGeneration
      .generateThreadTitle({
        cwd: process.cwd(),
        message: "Rename the settings page",
        modelSelection: createModelSelection(ProviderInstanceId.make("primeAgent"), "default"),
      })
      .pipe(Effect.flip);

    assert.equal(
      error.detail,
      "Prime Agent could not generate text: the model provider returned HTTP 401.",
    );
  }),
);
