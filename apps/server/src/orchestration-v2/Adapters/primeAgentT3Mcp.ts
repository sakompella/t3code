/**
 * Prime Agent's kernel reaches T3 through its own MCP client, which reads
 * generic MCP servers from the user's settings file only: project settings,
 * flags, the environment, and extensions cannot declare one. T3 only reads that
 * file; the user adds the entry. Until they do, the session has no T3 tools and
 * shows a setup notice. T3 never registers them natively, so the agent keeps
 * exactly one tool, `ipython`.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import { expandHomePath } from "../../pathExpansion.ts";
import { T3_MCP_BEARER_ENV } from "./piT3McpExtensionSource.ts";
import type { PiFlavor } from "./PiFlavor.ts";

const T3_CODE_MCP_SERVER_NAME = "t3-code";

/** The entry Prime Agent needs under `mcpServers` in its settings file. */
function t3CodeMcpServerEntry(endpoint: string) {
  return { type: "http", url: endpoint, bearerTokenEnvVar: T3_MCP_BEARER_ENV } as const;
}

function stripTrailingSlashes(url: string): string {
  return url.replace(/\/+$/, "");
}

/**
 * Whether the settings text declares a usable `t3-code` server for this
 * endpoint. A different port reaches another server, and a different token
 * variable would send no credential, so both count as undeclared.
 */
export function isT3CodeMcpDeclared(settingsJson: string | undefined, endpoint: string): boolean {
  if (settingsJson === undefined) return false;
  let settings: unknown;
  try {
    settings = JSON.parse(settingsJson);
  } catch {
    return false;
  }
  const servers = (settings as { readonly mcpServers?: unknown } | null)?.mcpServers;
  const entry = (servers as Record<string, unknown> | null | undefined)?.[T3_CODE_MCP_SERVER_NAME];
  if (typeof entry !== "object" || entry === null) return false;
  const { type, url, bearerTokenEnvVar, enabled } = entry as Record<string, unknown>;
  return (
    type === "http" &&
    typeof url === "string" &&
    stripTrailingSlashes(url) === stripTrailingSlashes(endpoint) &&
    bearerTokenEnvVar === T3_MCP_BEARER_ENV &&
    enabled !== false
  );
}

export function t3CodeMcpSetupHint(input: {
  readonly displayName: string;
  readonly settingsPath: string;
  readonly endpoint: string;
}): string {
  const entry = JSON.stringify({ [T3_CODE_MCP_SERVER_NAME]: t3CodeMcpServerEntry(input.endpoint) });
  return [
    `${input.displayName} reaches T3 Code through the \`${T3_CODE_MCP_SERVER_NAME}\` MCP server, which is not declared in ${input.settingsPath}. This session has no T3 Code tools.`,
    `To enable them, add this entry to "mcpServers" in that file and start a new session: ${entry}`,
    "The URL holds this server's port; update it if the port changes.",
  ].join("\n\n");
}

export type KernelMcpAccess =
  | { readonly declared: true }
  | { readonly declared: false; readonly hint: string };

/** Reads the agent's settings file and reports whether the kernel can reach T3. */
export const resolveKernelMcpAccess = Effect.fn("resolveKernelMcpAccess")(function* (input: {
  readonly displayName: string;
  readonly kernelMcp: NonNullable<PiFlavor["kernelMcp"]>;
  readonly environment: NodeJS.ProcessEnv;
  readonly endpoint: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const { agentDirEnvVar, defaultAgentDir } = input.kernelMcp;
  const agentDir = expandHomePath(input.environment[agentDirEnvVar] || `~/${defaultAgentDir}`);
  const settingsPath = `${agentDir.replace(/[\\/]+$/, "")}/settings.json`;
  const settingsJson = yield* fs.readFileString(settingsPath).pipe(
    Effect.map((text): string | undefined => text),
    Effect.orElseSucceed(() => undefined),
  );
  if (isT3CodeMcpDeclared(settingsJson, input.endpoint)) {
    return { declared: true } satisfies KernelMcpAccess;
  }
  return {
    declared: false,
    hint: t3CodeMcpSetupHint({
      displayName: input.displayName,
      settingsPath,
      endpoint: input.endpoint,
    }),
  } satisfies KernelMcpAccess;
});
