// @vitest-environment jsdom

import {
  DEFAULT_UNIFIED_SETTINGS,
  EnvironmentId,
  ThreadId,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  useParams,
} from "@tanstack/react-router";
import { useCallback, useEffect, useRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { useComposerDraftStore } from "../composerDraftStore";
import { makeThreadFixture } from "../test-fixtures";
import { ChatComposer, type ChatComposerHandle, type ChatComposerProps } from "./chat/ChatComposer";
import { SidebarThreadRow } from "./Sidebar";

const environmentId = EnvironmentId.make("environment-test");
const threads = {
  a: makeThreadFixture({ environmentId, id: ThreadId.make("thread-a"), title: "Thread A" }),
  b: makeThreadFixture({ environmentId, id: ThreadId.make("thread-b"), title: "Thread B" }),
};
type ThreadKey = keyof typeof threads;
const refFor = (key: ThreadKey): ScopedThreadRef => ({ environmentId, threadId: threads[key].id });
const promptOf = (key: ThreadKey) =>
  useComposerDraftStore.getState().getComposerDraft(refFor(key))?.prompt ?? "";
const noop = () => {};

type Router = ReturnType<typeof makeRouter>;
let router: Router;
let root: Root;
let container: HTMLDivElement;

/**
 * What ChatView gives the composer across a thread switch: one composer
 * instance for every server thread, and focus at the end of the new thread's
 * prompt on the next frame.
 */
function ThreadView() {
  const { threadKey } = useParams({ strict: false }) as { threadKey: ThreadKey };
  const thread = threads[threadKey];
  const threadRef = refFor(threadKey);
  const promptRef = useRef("");
  const composerRef = useRef<ChatComposerHandle | null>(null);
  const attachmentsRef = useRef([]);
  const focusComposer = useCallback(() => composerRef.current?.focusAtEnd(), []);
  useEffect(() => {
    const frame = requestAnimationFrame(focusComposer);
    return () => cancelAnimationFrame(frame);
  }, [thread.id, focusComposer]);

  const props: ChatComposerProps = {
    composerDraftTarget: threadRef,
    environmentId,
    attachmentUploadsCapabilityKnown: true,
    supportsAttachmentUploads: false,
    supportsQuestionAttachments: false,
    maxFileAttachmentBytes: null,
    routeKind: "server",
    routeThreadRef: threadRef,
    draftId: null,
    multipleModelSelections: null,
    supportsMultipleModels: false,
    onMultipleModelSelectionsChange: noop,
    activeThreadId: thread.id,
    activeThreadEnvironmentId: environmentId,
    activeThread: thread,
    activeThreadShell: thread,
    promptHistoryMessages: [],
    isServerThread: true,
    isLocalDraftThread: false,
    forceExpandedOnMobile: false,
    projectSelectionRequired: false,
    phase: "ready",
    canInterrupt: false,
    isConnecting: false,
    isSendBusy: false,
    canResume: false,
    sendDisabledReason: null,
    isPreparingWorktree: false,
    bannerItems: [],
    environmentUnavailable: null,
    activePendingApproval: null,
    pendingApprovals: [],
    pendingUserInputs: [],
    activePendingProgress: null,
    activePendingResolvedAnswers: null,
    activePendingIsResponding: false,
    activePendingDraftAnswers: {},
    activePendingQuestionIndex: 0,
    respondingRequestIds: [],
    showPlanFollowUpPrompt: false,
    activeProposedPlan: null,
    activeTasksProgress: null,
    activeTaskSteps: null,
    threadSyncPhase: null,
    runtimeMode: "full-access",
    interactionMode: "default",
    lockedProvider: null,
    providerStatuses: [],
    providerCatalogKnown: true,
    activeProjectId: null,
    activeProjectDefaultModelSelection: null,
    activeThreadModelSelection: thread.modelSelection,
    activeContextWindow: null,
    compactThreadUnavailable: true,
    compactDisabled: true,
    compactDisabledReason: null,
    resolvedTheme: "light",
    settings: DEFAULT_UNIFIED_SETTINGS,
    keybindings: [],
    terminalOpen: false,
    gitCwd: null,
    pullRequestProjectId: null,
    pullRequestRepository: null,
    threadPullRequests: undefined,
    restingControlsHost: null,
    restingControlsHaveLeadingContext: false,
    onRestingControlsVisibilityChange: noop,
    getTimelineScrollableNode: () => null,
    isTimelineAtLogicalEnd: () => true,
    timelineOverflows: false,
    onComposerOverlayHeightChange: noop,
    onRestingChange: noop,
    promptRef,
    composerImagesRef: attachmentsRef,
    composerFilesRef: attachmentsRef,
    composerTerminalContextsRef: attachmentsRef,
    composerRef,
    onPageScrollKeyDown: noop,
    onPageScrollKeyUp: noop,
    onPageScrollRelease: noop,
    editingQueuedAttachments: null,
    onRemoveEditingQueuedAttachment: noop,
    onCompactContext: noop,
    onSend: noop,
    onResume: noop,
    onInterrupt: noop,
    onImplementPlanInNewThread: noop,
    onRespondToApproval: async () => undefined,
    onSelectActivePendingUserInputOption: noop,
    onAdvanceActivePendingUserInput: noop,
    onDismissActivePendingUserInput: noop,
    onPreviousActivePendingUserInputQuestion: noop,
    onChangeActivePendingUserInputCustomAnswer: noop,
    onProviderModelSelect: noop,
    onOpenProviderSetup: noop,
    getModelDisabledReason: () => null,
    toggleInteractionMode: noop,
    handleRuntimeModeChange: noop,
    handleInteractionModeChange: noop,
    focusComposer,
    scheduleComposerFocus: () => void requestAnimationFrame(focusComposer),
    setThreadError: noop,
    onExpandImage: noop,
    onFileOpen: noop,
    canOperateThread: true,
    resumeCompactionTokens: null,
    keepFullHistory: false,
    onToggleKeepFullHistory: noop,
  };
  return (
    <>
      <ul>
        <SidebarThreadRow
          thread={threads.b}
          variant="card"
          variantAction="settle"
          scratchMachineLabel={null}
          settlementSupported={false}
          snoozeSupported={false}
          pinningSupported={false}
          isPinned={false}
          dropVerb={null}
          dragOverPinned={false}
          sweepAction={null}
          snoozeWakeLabelText={null}
          wokeAt={null}
          isActive={threadKey === "b"}
          openPullRequestsInRightPanel={false}
          jumpLabel={null}
          currentEnvironmentId={environmentId}
          environmentLabel={null}
          environmentMachine="laptop"
          project={null}
          projectDisplayName={null}
          providerEntryByInstanceId={new Map()}
          timestampFormat="locale"
          onThreadClick={() => void router.navigate({ href: "/b" })}
          onThreadActivate={() => void router.navigate({ href: "/b" })}
          onStartRename={noop}
          onRenameTitleChange={noop}
          onCommitRename={noop}
          onCancelRename={noop}
          isRenaming={false}
          renamingTitle=""
          onContextMenu={noop}
          onSettle={noop}
          onActionSweepStart={noop}
          onUnsettle={noop}
          onSnooze={noop}
          onUnsnooze={noop}
          onUnpin={noop}
          onAcknowledgeWoke={noop}
          changeRequestSnapshot={null}
          onChangeRequestSnapshot={noop}
        />
      </ul>
      <ChatComposer {...props} />
    </>
  );
}

function makeRouter() {
  const rootRoute = createRootRoute();
  const threadRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/$threadKey",
    component: ThreadView,
  });
  return createRouter({
    routeTree: rootRoute.addChildren([threadRoute]),
    history: createMemoryHistory({ initialEntries: ["/a"] }),
  });
}

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
const nextTask = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

function composerEditor() {
  const element = container.querySelector<HTMLElement & { editor?: { view: EditorView } }>(
    '[data-testid="composer-editor"]',
  );
  if (!element?.editor) throw new Error("composer editor is not mounted");
  return element;
}
interface EditorView {
  state: { tr: { insertText: (text: string) => unknown } };
  dispatch: (transaction: never) => void;
}

/**
 * A pointer press with the browser's default action, which jsdom leaves out:
 * an unprevented mousedown moves focus to the focusable element under it.
 */
function clickElement(target: HTMLElement) {
  const mousedown = new MouseEvent("mousedown", { bubbles: true, cancelable: true, detail: 1 });
  target.dispatchEvent(mousedown);
  if (!mousedown.defaultPrevented) target.closest<HTMLElement>("[tabindex]")?.focus();
  target.dispatchEvent(new MouseEvent("mouseup", { bubbles: true, cancelable: true, detail: 1 }));
  target.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, detail: 1 }));
}

/**
 * A key press with the browser's default action: text lands in the editor
 * only while the editor holds focus. Anywhere else it is lost, as it is on a
 * focused sidebar row, which ChatView's type-to-focus leaves alone.
 */
function pressKey(key: string) {
  const target = document.activeElement ?? document.body;
  const keydown = new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true });
  target.dispatchEvent(keydown);
  if (keydown.defaultPrevented || target !== composerEditor()) return;
  const { view } = composerEditor().editor!;
  view.dispatch(view.state.tr.insertText(key) as never);
}

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", false);
  vi.stubGlobal("matchMedia", (query: string) => ({
    matches: false,
    media: query,
    addEventListener: noop,
    removeEventListener: noop,
    addListener: noop,
    removeListener: noop,
  }));
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
  Object.defineProperty(document, "fonts", {
    configurable: true,
    value: { addEventListener: noop, removeEventListener: noop, ready: Promise.resolve() },
  });
  Range.prototype.getClientRects = () => [] as unknown as DOMRectList;
  Range.prototype.getBoundingClientRect = () => new DOMRect();
  Element.prototype.scrollIntoView = noop;

  useComposerDraftStore.getState().setPrompt(refFor("a"), "hi");
  useComposerDraftStore.getState().setPrompt(refFor("b"), "fix the bug");
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  router = makeRouter();
  await router.load();
  root.render(<RouterProvider router={router} />);
  await nextTask();
  await nextFrame();
  expect(document.activeElement).toBe(composerEditor());
});

afterEach(() => {
  root.unmount();
  container.remove();
  vi.unstubAllGlobals();
});

describe("typing right after clicking another thread in the sidebar", () => {
  it("lands every key in the clicked thread, in order", async () => {
    clickElement(container.querySelector<HTMLElement>('[data-testid="sidebar-row-card"]')!);
    for (const key of "please now") {
      await nextTask();
      pressKey(key);
    }
    await nextFrame();

    expect({ a: promptOf("a"), b: promptOf("b") }).toEqual({
      a: "hi",
      b: "fix the bugplease now",
    });
  });

  it("loses no key typed while the navigation is still settling", async () => {
    clickElement(container.querySelector<HTMLElement>('[data-testid="sidebar-row-card"]')!);
    for (const key of "please now") {
      await Promise.resolve();
      pressKey(key);
    }
    await nextFrame();

    const typedIntoA = promptOf("a").slice("hi".length);
    const typedIntoB = promptOf("b").slice("fix the bug".length);
    expect(typedIntoA + typedIntoB).toBe("please now");
  });

  it("still lets the keyboard focus a row and open it", async () => {
    const row = container.querySelector<HTMLElement>('[data-testid="sidebar-row-card"]')!;
    row.focus();
    expect(document.activeElement).toBe(row);
    pressKey("Enter");
    await nextTask();
    await nextFrame();

    expect(router.state.location.pathname).toBe("/b");
    expect(document.activeElement).toBe(composerEditor());
  });
});
