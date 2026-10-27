import type { HeartbeatPresentation } from "@t3tools/client-runtime/state/thread-heartbeats";
import type { ThreadId } from "@t3tools/contracts";
import { HeartPulseIcon } from "lucide-react";

import type { ComposerBannerStackItem } from "./ComposerBannerStack";

/**
 * A thread's heartbeat as a quiet composer notice: schedule and next run as
 * text, nothing that moves. ChatView lists it after the other notices.
 */
export function heartbeatBannerItem(
  threadId: ThreadId,
  presentation: HeartbeatPresentation,
): ComposerBannerStackItem {
  return {
    id: `heartbeat:${threadId}`,
    variant: "info",
    icon: <HeartPulseIcon />,
    title: presentation.title,
    ...(presentation.detail === null ? {} : { description: presentation.detail }),
  };
}
