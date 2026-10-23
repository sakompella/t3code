// @effect-diagnostics nodeBuiltinImport:off -- One synchronous stat of the file a command launches, like the command resolver in `@t3tools/shared/shell`.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as NodeFS from "node:fs";

/**
 * Names the file a command launches, so a result learned about it holds only
 * until the file changes. The key has the path the command resolves to (so
 * shims that share one real file, as mise and asdf make, keep their own
 * answers), the real path, and the real file's size, modification time,
 * change time and inode. Change time and inode catch an in-place replace that
 * restores the size and mtime, as `tar -x`, `cp -p` and `rsync -t` do; user
 * tools cannot set the change time. None when the command does not resolve to a
 * file T3 Code can stat.
 *
 * Limit: a shim or wrapper script that picks its target at run time launches a
 * file this key cannot see change. Such a command is identified once per
 * server process, until the wrapper file itself changes.
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
