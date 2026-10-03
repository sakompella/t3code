import type { HeartbeatPresentation } from "@t3tools/client-runtime/state/thread-heartbeats";
import type { ThreadId } from "@t3tools/contracts";
import { HeartPulseIcon } from "lucide-react";

import type { ComposerBannerStackItem } from "./ComposerBannerStack";

/**
 * A thread's heartbeat as a quiet composer notice: schedule and next run as
 * text, nothing that moves. It sits behind any activity or warning.
 */
export function heartbeatBannerItem(
  threadId: ThreadId,
  presentation: HeartbeatPresentation,
): ComposerBannerStackItem {
  return {
    id: `heartbeat:${threadId}`,
    variant: "info",
    priority: "notice",
    compact: true,
    icon: <HeartPulseIcon />,
    title: presentation.title,
    ...(presentation.detail === null ? {} : { description: presentation.detail }),
  };
}
