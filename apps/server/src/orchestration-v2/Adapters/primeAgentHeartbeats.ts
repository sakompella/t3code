import type { OrchestrationV2ProviderHeartbeat } from "@t3tools/contracts";
import { piRecordField as recordField, piRecordString as recordString } from "./PiRpc.ts";

const DESCRIPTION_LIMIT = 120;

/** The label the agent gave a heartbeat, or else the first line of what it runs. */
function describeHeartbeat(job: unknown): string | undefined {
  const label = recordString(job, "label")?.trim();
  if (label) return label;
  const firstLine = recordString(job, "prompt")
    ?.split("\n")
    .map((line) => line.trim())
    .find((line) => line.length > 0);
  if (firstLine === undefined) return undefined;
  return firstLine.length > DESCRIPTION_LIMIT
    ? `${firstLine.slice(0, DESCRIPTION_LIMIT - 1)}…`
    : firstLine;
}

/**
 * The heartbeats one Prime Agent session runs, from a `list_heartbeats`
 * reply. The reply lists every session's heartbeats, active and paused ones
 * alike; `get_heartbeat` is no use because it only sees the user's own
 * `/heartbeat`, not the ones the agent creates itself.
 */
export function primeAgentHeartbeats(
  listing: unknown,
  sessionId: string,
): ReadonlyArray<OrchestrationV2ProviderHeartbeat> {
  const entries = recordField(listing, "heartbeats");
  if (!Array.isArray(entries)) return [];
  return entries.flatMap((entry): ReadonlyArray<OrchestrationV2ProviderHeartbeat> => {
    const job = recordField(entry, "job");
    const id = recordString(job, "id");
    const status = recordString(job, "status");
    const schedule = recordString(recordField(job, "schedule"), "expression");
    if (
      id === undefined ||
      schedule === undefined ||
      recordString(job, "sessionId") !== sessionId ||
      (status !== "active" && status !== "paused")
    ) {
      return [];
    }
    const paused = status === "paused";
    const nextRunAt = paused ? undefined : recordString(job, "nextRunAt");
    const description = describeHeartbeat(job);
    return [
      {
        id,
        schedule,
        paused,
        ...(description === undefined ? {} : { description }),
        ...(nextRunAt === undefined || Number.isNaN(Date.parse(nextRunAt)) ? {} : { nextRunAt }),
      },
    ];
  });
}
