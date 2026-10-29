import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { OfflineEnvironmentFold } from "@t3tools/client-runtime/state/offline-environments";
import type { EnvironmentId } from "@t3tools/contracts";
import { ChevronDownIcon, CloudOffIcon } from "lucide-react";
import { memo } from "react";

import { cn } from "~/lib/utils";
import {
  FOLDER_DOT_CLASS_NAME,
  FOLDER_DOT_LABEL,
  useHiddenThreadsDot,
} from "./SidebarProjectFolderHeader";

/** The fold's sidebar tree row; its thread rows name it as their parent. */
export function sidebarOfflineFoldRowId(environmentId: EnvironmentId) {
  return `offline:${environmentId}`;
}

/**
 * The row an offline environment's threads fold into. It reads like a closed project folder:
 * a count and a status dot for the threads it hides, and a chevron that opens it.
 */
export const SidebarOfflineEnvironmentHeader = memo(
  function SidebarOfflineEnvironmentHeader(props: {
    fold: OfflineEnvironmentFold<EnvironmentThreadShell>;
    onToggle: (environmentId: EnvironmentId) => void;
  }) {
    const { fold } = props;
    const dot = useHiddenThreadsDot(fold.hiddenThreads);
    return (
      <li className="relative mt-2 list-none first:mt-0">
        <button
          type="button"
          role="treeitem"
          tabIndex={-1}
          data-sidebar-row={sidebarOfflineFoldRowId(fold.environmentId)}
          aria-expanded={fold.expanded}
          onClick={() => props.onToggle(fold.environmentId)}
          className="flex h-8 w-full cursor-pointer items-center gap-2.5 rounded-(--control-radius) px-(--sidebar-row-content-inset) text-left outline-none hover:bg-sidebar-row-hover focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
        >
          <CloudOffIcon aria-hidden className="size-4 shrink-0 text-sidebar-muted-foreground" />
          <span className="min-w-0 truncate text-sm font-medium text-sidebar-foreground/90">
            {fold.label}
          </span>
          <span className="min-w-0 flex-1 truncate text-xs text-sidebar-muted-foreground">
            offline
          </span>
          {dot ? (
            <span className={cn("size-1.5 shrink-0 rounded-full", FOLDER_DOT_CLASS_NAME[dot])}>
              <span className="sr-only">{FOLDER_DOT_LABEL[dot]}</span>
            </span>
          ) : null}
          {fold.hiddenThreads.length > 0 ? (
            <span className="shrink-0 text-xs tabular-nums text-sidebar-muted-foreground">
              {fold.hiddenThreads.length}
            </span>
          ) : null}
          <ChevronDownIcon
            aria-hidden
            className={cn(
              "size-3 shrink-0 text-sidebar-muted-foreground transition-transform",
              !fold.expanded && "-rotate-90",
            )}
          />
        </button>
      </li>
    );
  },
);
