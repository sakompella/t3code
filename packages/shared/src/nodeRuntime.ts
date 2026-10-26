import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as HostProcess from "./HostProcess.ts";
import { CommandResolutionCache, resolveCommandPath } from "./shell.ts";

const NodeRuntimeFeature = Schema.Literals([
  "Local device support",
  "Device automation",
  "Antigravity",
  "Antigravity sign-in",
]);

export const nodeRuntimeUnavailableMessage = (feature: typeof NodeRuntimeFeature.Type): string =>
  `${feature} requires Node.js. Install Node.js and make sure node is on PATH, then retry.`;

export class NodeRuntimeUnavailableError extends Schema.TaggedError<NodeRuntimeUnavailableError>()(
  "NodeRuntimeUnavailableError",
  { feature: NodeRuntimeFeature, cause: Schema.optional(Schema.Defect()) },
) {
  override get message(): string {
    return nodeRuntimeUnavailableMessage(this.feature);
  }
}

export interface SelfInvocation {
  readonly command: string;
  /** Absolute script entrypoint; undefined for packaged binaries, which dispatch subcommands from argv[2]. */
  readonly entrypoint: string | undefined;
}

/** Packaged binaries repeat the binary at argv[1]; callers must not assemble execPath/argv[1] themselves. */
export const resolveSelfInvocation = Effect.fn("nodeRuntime.resolveSelfInvocation")(function* () {
  const command = yield* HostProcess.ExecutablePath;
  if (yield* HostProcess.IsExecutable)
    return { command, entrypoint: undefined } satisfies SelfInvocation;
  const path = yield* Path.Path;
  const entry = (yield* HostProcess.Arguments)[1];
  // Children spawn from their own working directory, so the script path must be absolute.
  return {
    command,
    entrypoint: entry === undefined ? undefined : path.resolve(entry),
  } satisfies SelfInvocation;
});

export const selfInvocationArgs = (
  invocation: SelfInvocation,
  args: ReadonlyArray<string>,
): ReadonlyArray<string> =>
  invocation.entrypoint === undefined ? args : [invocation.entrypoint, ...args];

export interface NodeRuntime {
  /** A binary that runs JavaScript the way `node` does when given `env`. */
  readonly command: string;
  /**
   * The environment that makes `command` behave as Node: `ELECTRON_RUN_AS_NODE`
   * when it is the Electron binary, otherwise empty. Set it on the one child
   * that runs `command`; the server itself does not leak it to other children.
   */
  readonly env: Readonly<Record<string, string>>;
}

export const ELECTRON_RUN_AS_NODE_ENV = { ELECTRON_RUN_AS_NODE: "1" } as const;

export const resolveNodeRuntime = Effect.fn("nodeRuntime.resolveNodeRuntime")(function* (
  feature: typeof NodeRuntimeFeature.Type,
  environment?: NodeJS.ProcessEnv,
) {
  const executablePath = yield* HostProcess.ExecutablePath;
  if (!(yield* HostProcess.IsExecutable)) {
    const env: NodeRuntime["env"] = (yield* HostProcess.IsElectron) ? ELECTRON_RUN_AS_NODE_ENV : {};
    return { command: executablePath, env } satisfies NodeRuntime;
  }

  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;
  const env = environment ?? (yield* HostProcess.Environment);
  const resolvedNodePath = yield* resolveCommandPath(platform === "win32" ? "node.exe" : "node", {
    env: platform === "win32" ? { ...env, PATHEXT: ".EXE" } : env,
  }).pipe(Effect.provideService(CommandResolutionCache, new Map()));
  if (resolvedNodePath === null) return yield* new NodeRuntimeUnavailableError({ feature });
  const nodePath = path.resolve(resolvedNodePath);
  const resolvedPath = yield* fs
    .realPath(nodePath)
    .pipe(Effect.mapError((cause) => new NodeRuntimeUnavailableError({ feature, cause })));
  if (resolvedPath === executablePath) return yield* new NodeRuntimeUnavailableError({ feature });
  const [hostInfo, nodeInfo] = yield* Effect.all([
    fs.stat(executablePath).pipe(Effect.option),
    fs.stat(nodePath).pipe(Effect.option),
  ]);
  if (
    Option.isSome(hostInfo) &&
    Option.isSome(nodeInfo) &&
    hostInfo.value.dev === nodeInfo.value.dev &&
    Option.isSome(hostInfo.value.ino) &&
    Option.isSome(nodeInfo.value.ino) &&
    hostInfo.value.ino.value > 0n &&
    hostInfo.value.ino.value === nodeInfo.value.ino.value
  ) {
    return yield* new NodeRuntimeUnavailableError({ feature });
  }
  // Launchers such as Vite+ dispatch by argv[0]; keep the node name intact.
  return { command: nodePath, env: {} } satisfies NodeRuntime;
});

/** The command alone, for callers that only check Node is available. */
export const resolveNodeExecutable = Effect.fn("nodeRuntime.resolveNodeExecutable")(function* (
  feature: typeof NodeRuntimeFeature.Type,
  environment?: NodeJS.ProcessEnv,
) {
  return (yield* resolveNodeRuntime(feature, environment)).command;
});
