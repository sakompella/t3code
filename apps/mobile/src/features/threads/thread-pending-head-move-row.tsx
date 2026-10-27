import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";
import { useMemo } from "react";
import type { ColorValue } from "react-native";
import { environmentThreadDetails } from "../../state/threads";
import { ThreadContextDivider } from "./thread-context-divider";

/** A rollback another client started is still running on the server. */
export function ThreadPendingHeadMoveRow(props: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
  iconColor: ColorValue;
}) {
  const summarizeAtom = useMemo(() => {
    const ref = scopeThreadRef(props.environmentId, props.threadId);
    // Only whether and how the move runs matters here, not the request.
    return Atom.make((get) => {
      const pending = get(environmentThreadDetails.threadAtom(ref))?.projection.thread
        .pendingHeadMove;
      return pending ? pending.summarize : null;
    });
  }, [props.environmentId, props.threadId]);
  const summarize = useAtomValue(summarizeAtom);
  if (summarize === null) return null;
  return (
    <ThreadContextDivider
      label={summarize ? "Summarizing discarded turns…" : "Rewinding…"}
      icon="arrow.uturn.backward"
      iconColor={props.iconColor}
    />
  );
}
