import { presentHeartbeats } from "@t3tools/client-runtime/state/thread-heartbeats";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useAtomValue } from "@effect/atom-react";
import * as DateTime from "effect/DateTime";
import { View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { SymbolView } from "../../components/AppSymbol";
import { environmentThreadDetails } from "../../state/threads";

/**
 * The thread's heartbeat as one muted line above the composer: schedule and
 * next run as plain text. A heartbeat is configuration, so it neither animates
 * nor counts as work.
 */
export function ComposerHeartbeat(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const heartbeats = useAtomValue(environmentThreadDetails.heartbeatsAtom(props));
  const presentation = presentHeartbeats(heartbeats, DateTime.toDate(DateTime.nowUnsafe()));
  if (presentation === null) return null;
  const line =
    presentation.detail === null
      ? presentation.title
      : `${presentation.title} · ${presentation.detail}`;
  return (
    <View className="flex-row items-center gap-2 px-4 pb-2">
      <SymbolView name="clock" size={12} tintColorClassName="accent-foreground-muted" />
      <Text className="min-w-0 flex-1 text-xs text-foreground-muted" numberOfLines={1}>
        {line}
      </Text>
    </View>
  );
}
