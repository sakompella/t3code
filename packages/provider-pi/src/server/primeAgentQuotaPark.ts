import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

/**
 * Prime Agent parks a session when a quota reset exceeds its retry wait. Its RPC reports this only
 * in the retry outcome's text (`auto_retry_end.finalError`), on the turn that parks and on each
 * later turn the park refuses. `get_state` has no park field.
 */
const PARKED_UNTIL = /\bSession (?:is )?parked until (\d{4}-\d{2}-\d{2}T[\d:.]+Z)/;

/** When the park that ended a turn resumes the session, or null when the turn did not park. */
export function primeAgentQuotaParkResumeAt(finalError: string | undefined): DateTime.Utc | null {
  const iso = finalError?.match(PARKED_UNTIL)?.[1];
  return iso === undefined ? null : Option.getOrNull(DateTime.make(iso));
}

const resumeTimeFormat = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  timeZoneName: "short",
});

/** The server formats the time, so it names its zone for clients elsewhere. */
export function primeAgentQuotaParkNotice(resumeAt: DateTime.Utc, agentName: string): string {
  return `Waiting for quota until ${resumeTimeFormat.format(DateTime.toDate(resumeAt))}. ${agentName} resumes on its own.`;
}
