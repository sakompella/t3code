// @effect-diagnostics nodeBuiltinImport:off -- One synchronous stat of the file a command launches, like the command resolver in `@t3tools/shared/shell`.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as NodeFS from "node:fs";

/**
 * Names the file a command launches by its real path, size and modification
 * time, so a result learned about it holds only until the file changes. None
 * when the command does not resolve to a file T3 Code can stat.
 */
export const binaryFileKey = (command: string, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const platform = yield* HostProcess.Platform;
    const resolveExecutable = yield* SpawnExecutableResolution;
    const executable = resolveExecutable(command, platform, environment);
    if (executable === undefined) return Option.none<string>();
    return yield* Effect.try(() => {
      const realPath = NodeFS.realpathSync(executable);
      const { size, mtimeMs } = NodeFS.statSync(realPath);
      return [realPath, size, mtimeMs].join("\0");
    }).pipe(Effect.option);
  });
