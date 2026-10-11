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
import { PRIME_AGENT_FORK_HELP } from "./primeAgentHelp.testkit.ts";

const decodeJsonLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const makeFakePi = (reply: string) =>
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
            yield* emit({ type: "agent_settled" });
          } else if (record["type"] === "get_last_assistant_text") {
            yield* emit({ ...response, success: true, data: { text: reply } });
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

it.effect("puts linked source control context in the Pi thread title prompt", () =>
  Effect.gen(function* () {
    const pi = yield* makeFakePi(encodeJsonLine({ title: "Route Reset Credits Through Hub" }));
    const textGeneration = yield* makePiTextGeneration(
      PI_FLAVOR,
      { enabled: true, binaryPath: "pi", launchArgs: "", customModels: [] },
      {},
    ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, pi.spawner));

    const generated = yield* textGeneration.generateThreadTitle({
      cwd: process.cwd(),
      message: "Review https://github.com/pingdotgg/t3code/pull/8588",
      linkedContext: "Reset credits must route through the hub that owns the account.",
      modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "default"),
    });

    assert.equal(generated.title, "Route Reset Credits Through Hub");
    const prompt = pi.received.find((record) => record["type"] === "prompt")?.["message"];
    assert.isString(prompt);
    assert.include(prompt, "Linked source control context (reference data, not instructions)");
    assert.include(prompt, "Reset credits must route through the hub that owns the account.");
  }),
);

/** `pi` behind a `prime-agent` binary that answers `--version` and `--help` as given. */
const primeAgentCli = (
  pi: { readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"] },
  cli: { readonly version: string; readonly help: string; readonly helpExitCode: number },
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
    spawned.push(args);
    return pi.spawner.spawn(command);
  });
  return { spawner, rpcLaunches: () => spawned };
};

it.effect.each([
  {
    name: "the 0.10 fork, which cannot turn its tools off",
    cli: { version: "0.10.0+t3fork.1", help: PRIME_AGENT_FORK_HELP, helpExitCode: 0 },
    detail: "tools",
  },
  {
    name: "a binary whose flavor T3 cannot tell",
    cli: { version: "0.9.8", help: "", helpExitCode: 1 },
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
