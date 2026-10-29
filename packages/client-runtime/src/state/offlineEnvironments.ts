import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { Atom } from "effect/reactivity";

import type { EnvironmentPresentation } from "../connection/presentation.ts";

/**
 * Whether a saved environment's threads fold away: the client tried to reach it, failed, and
 * has not connected since. A session that just dropped has no failed attempt yet while it
 * reconnects, so a short drop does not move rows. The primary environment never folds.
 */
export function isOfflineFoldedEnvironment(presentation: EnvironmentPresentation): boolean {
  if (presentation.entry.target._tag === "PrimaryConnectionTarget") return false;
  const { phase, error } = presentation.connection;
  return (
    phase === "offline" ||
    phase === "error" ||
    phase === "unsupported" ||
    (phase === "reconnecting" && error !== null)
  );
}

/** Labels of the environments whose threads fold away, keyed by environment. */
export function createOfflineEnvironmentLabelsAtom(
  presentationsAtom: Atom.Atom<ReadonlyMap<EnvironmentId, EnvironmentPresentation>>,
) {
  return Atom.make((get) => {
    const next = new Map<EnvironmentId, string>();
    for (const [environmentId, presentation] of get(presentationsAtom)) {
      if (isOfflineFoldedEnvironment(presentation)) {
        next.set(environmentId, presentation.entry.target.label);
      }
    }
    const previous = Option.getOrNull(get.self<ReadonlyMap<EnvironmentId, string>>());
    return previous !== null &&
      previous.size === next.size &&
      [...next].every(([environmentId, label]) => previous.get(environmentId) === label)
      ? previous
      : next;
  }).pipe(Atom.withLabel("offline-environment-labels"));
}

export interface OfflineEnvironmentFold<T> {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly expanded: boolean;
  /** Every thread under the fold's row while it is open, else only the open thread. */
  readonly shownThreads: ReadonlyArray<T>;
  /** The threads the closed fold hides; empty while it is open. */
  readonly hiddenThreads: ReadonlyArray<T>;
}

/**
 * Moves the threads of offline environments out of a list into one fold per environment, in
 * the order the environments first appear. Folded threads keep their list order, and a closed
 * fold still shows the open thread.
 */
export function foldOfflineEnvironmentThreads<
  T extends { readonly environmentId: EnvironmentId; readonly id: ThreadId },
>(input: {
  readonly threads: ReadonlyArray<T>;
  readonly offlineEnvironmentLabels: ReadonlyMap<EnvironmentId, string>;
  readonly expandedEnvironmentIds: ReadonlySet<EnvironmentId>;
  readonly openThread: {
    readonly environmentId: EnvironmentId;
    readonly threadId: ThreadId;
  } | null;
}): {
  readonly threads: ReadonlyArray<T>;
  readonly folds: ReadonlyArray<OfflineEnvironmentFold<T>>;
} {
  const { offlineEnvironmentLabels, expandedEnvironmentIds, openThread } = input;
  if (offlineEnvironmentLabels.size === 0) return { threads: input.threads, folds: [] };
  const listed: T[] = [];
  const foldedByEnvironment = new Map<EnvironmentId, T[]>();
  for (const thread of input.threads) {
    if (!offlineEnvironmentLabels.has(thread.environmentId)) {
      listed.push(thread);
      continue;
    }
    const folded = foldedByEnvironment.get(thread.environmentId);
    if (folded === undefined) foldedByEnvironment.set(thread.environmentId, [thread]);
    else folded.push(thread);
  }
  const isOpen = (thread: T) =>
    openThread !== null &&
    thread.environmentId === openThread.environmentId &&
    thread.id === openThread.threadId;
  const folds = [...foldedByEnvironment].map(([environmentId, threads]) => {
    const expanded = expandedEnvironmentIds.has(environmentId);
    return {
      environmentId,
      label: offlineEnvironmentLabels.get(environmentId)!,
      expanded,
      shownThreads: expanded ? threads : threads.filter(isOpen),
      hiddenThreads: expanded ? [] : threads.filter((thread) => !isOpen(thread)),
    };
  });
  return { threads: listed, folds };
}
