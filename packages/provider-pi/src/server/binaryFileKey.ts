// @effect-diagnostics nodeBuiltinImport:off -- One synchronous stat of the file a command launches, like the command resolver in `@t3tools/shared/shell`.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as NodeFS from "node:fs";

/**
 * Names the file a command launches, so a learned result holds only until the file changes. The key
 * has the resolved path (shims sharing one real file keep their own answers), the real path, and
 * its size, mtime, ctime and inode; ctime and inode catch in-place replaces that restore size and
 * mtime. Limit: a wrapper that picks its target at run time hides changes of that target.
 */
export const binaryFileKey = (command: string, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const platform = yield* HostProcess.Platform;
    const resolveExecutable = yield* SpawnExecutableResolution;
    const executable = resolveExecutable(command, platform, environment);
    if (executable === undefined) return Option.none<string>();
    return yield* Effect.try(() => {
      const realPath = NodeFS.realpathSync(executable);
      const { size, mtimeMs, ctimeMs, ino, dev } = NodeFS.statSync(realPath);
      return [executable, realPath, size, mtimeMs, ctimeMs, ino, dev].join("\0");
    }).pipe(Effect.option);
  });
