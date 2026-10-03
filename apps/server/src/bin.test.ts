// @effect-diagnostics nodeBuiltinImport:off - the child must be a real OS process to observe its environment.
import { describe, expect, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";

describe("server entry", () => {
  it("does not hand ELECTRON_RUN_AS_NODE to the children it spawns", () => {
    const binUrl = new URL("./bin.ts", import.meta.url).href;
    const script = [
      `await import(${JSON.stringify(binUrl)});`,
      `const { execFileSync } = await import("node:child_process");`,
      `const seen = execFileSync(process.execPath, ["-p", "process.env.ELECTRON_RUN_AS_NODE ?? 'unset'"], { encoding: "utf8" });`,
      `console.log(seen.trim());`,
    ].join("\n");
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      ["--input-type=module", "-e", script],
      {
        env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
        encoding: "utf8",
      },
    );
    expect(result.stdout.trim()).toBe("unset");
  });
});
