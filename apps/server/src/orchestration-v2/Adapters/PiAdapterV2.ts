/**
 * PiAdapterV2 — orchestrator-v2 adapter for the Pi coding agent
 * (https://pi.dev), driving `pi --mode rpc` over stdio JSONL via `PiRpc.ts`.
 *
 * Design intent: honor the user's Pi customizations. The process is spawned
 * with no `--no-*` flags, so the user's extensions, skills, prompt templates,
 * AGENTS.md / SYSTEM.md context, settings.json, custom models, and auth all
 * load exactly as they do in the `pi` TUI. Sessions are stored by Pi itself
 * (default `~/.pi/agent/sessions/`), and the session file path is the durable
 * `nativeThreadRef`, so a thread started in T3 can be resumed from the TUI
 * and vice versa.
 *
 * Turn lifecycle: `agent_settled` is the only terminal signal. `agent_end`
 * merely closes one low-level run — compaction retries, auto-retries, and
 * queued continuations may still follow it, so the turn stays open until Pi
 * reports the session settled. An extension can start detached compaction as
 * that signal unwinds, so the adapter confirms Pi is idle before terminalizing.
 *
 * Extension UI: Pi extensions raise dialogs through `extension_ui_request`.
 * Dialog methods become v2 runtime requests (`confirm` → approval_request,
 * `select`/`input`/`editor` → user_input_request); answers travel back as
 * `extension_ui_response`. `notify` becomes a completed activity item.
 * Terminal-only decoration such as status, widget, title, and editor-text
 * updates has no matching T3 surface and is ignored.
 */
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import {
  defaultInstanceIdForDriver,
  PiSettings,
  PrimeAgentSettings,
  type ChatAttachment,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2Notification,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderRetry,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserInputQuestion,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
  type ProviderThreadId,
  type ProviderTurnId,
  type RunId,
  type ThreadId,
  type OrchestrationV2ProviderTurnTokenUsage,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import * as ServerConfig from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  expandPiSkillReference,
  parsePiCompactCommand,
  hasPiNavigateTreeCommand,
  parsePiDiscoveredCommands,
  type PiCompactCommand,
} from "../../provider/PiCommands.ts";
import { mergeProviderInstanceEnvironment } from "../../provider/ProviderInstanceEnvironment.ts";
import * as IdAllocator from "../IdAllocator.ts";
import * as ProviderAdapter from "../ProviderAdapter.ts";
import * as ProviderContinuationRequests from "../ProviderContinuationRequests.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "../ProviderAdapterDriver.ts";
import { makeProviderFailure, makeProviderRetryTurnItem } from "../ProviderFailure.ts";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
  subagentThreadTitle,
} from "../SubagentProjection.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";
import {
  makePiRpcConnection,
  parsePiModelSlug,
  piRecordField as recordField,
  piRecordNumber as recordNumber,
  piRecordString as recordString,
  piStateIsIdle,
  type PiRpcConnection,
  type PiRpcRecord,
} from "./PiRpc.ts";
import {
  buildPiRpcLaunch,
  materializePiT3McpExtension,
  resolvePiLaunchArgs,
} from "./piT3McpInjection.ts";
import {
  PI_FILE_CHANGE_TOOLS,
  T3_NAVIGATE_TREE_COMMAND,
  T3_NAVIGATE_TREE_RESULT_MARKER,
} from "./piT3McpExtensionSource.ts";
import { PI_FLAVOR, PRIME_AGENT_FLAVOR, type PiFlavor } from "./PiFlavor.ts";
import {
  awaitsHandle,
  classifyIpythonCell,
  reportedCommandMatches,
  detachedBashJobs,
  previewPythonCell,
  type DetachedBashJob,
} from "./primeAgentIpythonCell.ts";

export const PI_PROVIDER = PI_FLAVOR.driverKind;
const PI_DEFAULT_INSTANCE_ID = defaultInstanceIdForDriver(PI_PROVIDER);
const DEFAULT_PI_SETTINGS = Schema.decodeSync(PiSettings)({});

/**
 * Sentinel model slug meaning "do not call set_model": Pi resolves the model
 * from the user's own settings.json (`defaultProvider`/`defaultModel`).
 */
const PI_INHERIT_MODEL_SLUG = "default";

const STREAM_FLUSH_MS = 50;
const FINISHING_UP_LABEL = "Finishing up…";
const FINISHED_UP_LABEL = "Finished up";
const FINISHING_UP_DELAY = Duration.millis(1_500);
const PI_REQUEST_TIMEOUT_MS = 15_000;
// Session lifecycle hooks reload extensions, MCP servers and language servers.
const PI_SESSION_TIMEOUT_MS = 60_000;
const PI_SKILL_DISCOVERY_TIMEOUT_MS = 4_000;
const SETTLE_PROBE_MAX_ATTEMPTS = 3;
const SETTLE_PROBE_RETRY_DELAY = Duration.millis(100);
/** Idle-probe flavors re-read state while a retry, compaction, or queued action is still running. */
const BUSY_PROBE_INITIAL_DELAY_MS = 100;
const BUSY_PROBE_MAX_DELAY_MS = 1_000;

const PiProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    // Mode changes restart this process so the injected permission hook gets
    // one immutable policy for its whole lifetime.
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: true,
    canForkThread: true,
    canForkFromTurn: true,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    // Pi exposes a blocking tool_call extension hook. The T3 bridge uses it
    // for supervised and auto-accept modes and forwards its confirmations
    // through the same extension UI protocol as user-installed extensions.
    supportsCommandApproval: true,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    // T3 delegation uses the shared MCP `delegate_task` path. Installed Pi
    // subagent extensions are observed best-effort, but their official tool
    // runs children with --no-session and exposes no resumable child id.
    supportsSubagents: true,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    // T3 delivers both full and delta handoffs through Pi's normal user-message
    // input, so neither strategy depends on a Pi-specific context hook.
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: true,
    // CommandPolicy.ensureRollback requires the snapshot whenever provider
    // rollback is enabled; rollbackThread returns the updated provider thread.
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface PiAdapterV2Options {
  readonly flavor: PiFlavor;
  readonly instanceId: ProviderInstanceId;
  readonly settings: PiSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly fileSystem: FileSystem.FileSystem;
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly serverConfig: ServerConfig.ServerConfig["Service"];
  /** Receives wake turns the agent started on its own; defaults to dropping them. */
  readonly continuationRequests?: {
    readonly offer: (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) => Effect.Effect<void>;
  };
}

/** Concatenate the `text` fields of a Pi content-block array. */
function contentText(content: unknown): string {
  if (!Array.isArray(content)) {
    return typeof content === "string" ? content : "";
  }
  return content
    .map((block) => {
      if (recordField(block, "type") === "text") return recordString(block, "text") ?? "";
      return "";
    })
    .join("");
}

/** The streamable text a Pi message snapshot holds for one content block. */
function snapshotBlock(
  message: unknown,
  contentIndex: number,
): { readonly kind: PiStreamItemState["kind"]; readonly text: string } | undefined {
  const content = recordField(message, "content");
  if (!Array.isArray(content)) return undefined;
  const block: unknown = content[contentIndex];
  const type = recordField(block, "type");
  const text =
    type === "text"
      ? recordString(block, "text")
      : type === "thinking"
        ? recordString(block, "thinking")
        : undefined;
  if (text === undefined || text.length === 0) return undefined;
  return { kind: type === "text" ? "assistant_message" : "reasoning", text };
}

function pythonCellTitle(code: string): string {
  const preview = previewPythonCell(code);
  return preview.length === 0 ? "Python" : `Python: ${preview}`;
}

/**
 * Prime Agent runs every tool as an `ipython` cell, so only the T3 bridge's
 * own confirmation can gate it. Edits are only known from the cell's result,
 * after it ran, so there is no file-change approval to skip.
 */
function piProviderCapabilities(flavor: PiFlavor): OrchestrationV2ProviderCapabilities {
  if (flavor.tools === "pi") return PiProviderCapabilitiesV2;
  return {
    ...PiProviderCapabilitiesV2,
    approvals: { ...PiProviderCapabilitiesV2.approvals, supportsFileChangeApproval: false },
  };
}

const PI_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

// ── per-session state ─────────────────────────────────────────

interface PiStreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  readonly startedAt: DateTime.Utc;
}

type PiCompactionStatus = "running" | "completed" | "failed" | "cancelled";

interface PiCompactionState {
  readonly nativeItemId: string;
  readonly startedAt: DateTime.Utc;
}

interface PiProviderRetryState {
  readonly retry: OrchestrationV2ProviderRetry;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinal: number;
}

function compactionTitle(status: PiCompactionStatus): string {
  switch (status) {
    case "running":
      return "Compacting context...";
    case "completed":
      return "Context compacted";
    case "failed":
      return "Context compaction failed";
    case "cancelled":
      return "Context compaction stopped";
  }
}

interface ActivePiTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  /** Increments on assistant `message_start` so content indexes stay unique. */
  messageOrdinal: number;
  readonly streamItems: Map<string, PiStreamItemState>;
  readonly toolArgs: Map<string, unknown>;
  /**
   * First-seen time per `toolCallId`. Later update/end events reuse it so a
   * tool keeps one start timestamp and reports a real duration.
   */
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  interrupted: boolean;
  /**
   * Whether any agent run activity was observed. Command-only prompts (pure
   * extension slash commands) never start an agent run and never emit
   * `agent_settled`; their deferred prompt ack plus an idle probe settles
   * the turn instead.
   */
  sawAgentActivity: boolean;
  /** Only slash-command prompts can complete without starting an agent run. */
  readonly promptMayBeCommandOnly: boolean;
  /** Pi reports context as unknown immediately after compaction; keep its estimate for the meter. */
  latestCompactionAfterTokens: number | null;
  /** Last streamed usage total already emitted on the running turn. */
  lastLiveUsedTokens: number | null;
  /** Invalidates idle snapshots when new work starts after a settle probe. */
  settleProbeGeneration: number;
  /** An extension may start compaction immediately after Pi emits agent_settled. */
  settleWhenIdle: boolean;
  sawCompaction: boolean;
  /** RPC compact is in flight; Pi abort does not cancel it. */
  manualCompactInFlight: boolean;
  activeCompaction: PiCompactionState | null;
  activeProviderRetry: PiProviderRetryState | null;
  /**
   * The "Finishing up…" row shown between the final reply and the end of the
   * run, while Prime Agent still reviews its own harness.
   */
  finishingUp: {
    readonly nativeItemId: string;
    readonly startedAt: DateTime.Utc;
    /** The row is only shown once the wait runs long enough to notice. */
    shown: boolean;
  } | null;
  /** Counts synthetic notices so each gets its own id within the turn. */
  noticeCount: number;
  failure: ReturnType<typeof makeProviderFailure> | null;
  /** Session-tree refs read just before Stop terminates Pi, when no read is possible later. */
  stopTreeRefs?: PiTurnTreeRefs | null;
  /**
   * The turn's start entry, read once its prompt is persisted. A fork from an
   * earlier turn needs it while this turn still runs, and a later read would
   * mistake a steer's user entry for the turn start.
   */
  startEntryId?: string | null;
}

interface PiTurnTreeRefs {
  readonly turnStartEntryId: string | null;
  readonly leafId: string | null;
}

interface PendingPiPrompt {
  readonly nativeRequestId: string;
  readonly method: "select" | "confirm" | "input" | "editor";
  readonly questionId: string;
  readonly approvalKey: string;
  runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly turnItem: OrchestrationV2TurnItem;
}

/**
 * The T3 bridge confirms tool calls as `Allow <tool>?`. Edits surface as
 * file-change approvals so clients render them like other providers' edits;
 * every other confirmation, including ones from user extensions, is a command.
 */
function piApprovalRequestKind(title: string): "command" | "file-change" {
  const toolName = /^Allow (\S+)\?$/.exec(title)?.[1];
  return toolName !== undefined &&
    (PI_FILE_CHANGE_TOOLS as ReadonlyArray<string>).includes(toolName)
    ? "file-change"
    : "command";
}

const decodeNavigateTreeResult = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

function parseNavigateTreeResult(json: string): unknown {
  return Option.getOrUndefined(decodeNavigateTreeResult(json));
}

interface PendingPiWake {
  readonly events: Array<PiRpcRecord>;
  offered: boolean;
  readonly generation: number;
}

/**
 * Agent work that belongs to a wake turn. Dialogs, acks, and child session
 * observation keep flowing live: a child's events must not wait on the
 * parent's turn, or its roster update can drop the route first.
 */
function isPiWakeEvent(event: PiRpcRecord): boolean {
  switch (event["type"]) {
    case "response":
    case "rlm_child_update":
    case "observed_session_event":
    case "observed_session_closed":
    case "extension_ui_request":
    case "extension_error":
    case "t3.settle_probe":
    case "t3.flush_extension_errors":
      return false;
    default:
      return true;
  }
}

/**
 * Says what woke the agent, from the first message it was woken with that has
 * a known meaning. Bookkeeping notices such as `ipython_state_restored` are
 * skipped.
 */
function piWakeNotification(
  events: ReadonlyArray<PiRpcRecord>,
  agentName: string,
): OrchestrationV2Notification {
  for (const event of events) {
    if (event["type"] !== "message_start") continue;
    const message = event["message"];
    if (recordString(message, "role") === "assistant") break;
    const customType = recordString(message, "customType");
    if (customType === "agent_message") {
      const details = recordField(message, "details");
      const sender = recordString(recordField(details, "from"), "sessionName") ?? "an agent";
      const text = recordString(details, "message");
      return {
        source:
          recordString(details, "fromRelationship") === "child"
            ? { kind: "subagent" }
            : { kind: "background_task" },
        outcome: "updated",
        summary: `Message from ${sender}`,
        ...(text === undefined ? {} : { detail: text.slice(0, 2_000) }),
      };
    }
    if (customType === "async_bash_completion") {
      return {
        source: { kind: "command" },
        outcome: "completed",
        summary: "Background command finished",
      };
    }
    if (customType === "heartbeat_prompt") {
      return { source: { kind: "background_task" }, outcome: "updated", summary: "Heartbeat" };
    }
    if (customType === "rlm_child_failure") {
      return { source: { kind: "subagent" }, outcome: "failed", summary: "Subagent failed" };
    }
    if (customType === "rlm_child_terminal_notice") {
      return { source: { kind: "subagent" }, outcome: "completed", summary: "Subagent finished" };
    }
  }
  return {
    source: { kind: "background_task" },
    outcome: "updated",
    summary: `${agentName} resumed work`,
  };
}

interface PiRlmChildState {
  readonly snapshot: unknown;
  readonly startedAt: DateTime.Utc;
  readonly terminal: boolean;
  readonly turn: ActivePiTurn;
}

/** Where emitted items land: a turn on the parent thread, or a child's own thread. */
interface PiItemScope {
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly rootNodeId: NodeId;
  readonly providerThreadId: ProviderThreadId | null;
  readonly providerTurnId: ProviderTurnId | null;
  /** Keeps native item ids unique per turn or child. */
  readonly idPrefix: string;
}

/**
 * The transcript of one Prime Agent child, kept in the child's own T3 thread.
 * It carries the same item bookkeeping as `ActivePiTurn`, so the turn's item
 * emitters serve both.
 */
interface PiChildTranscript {
  readonly scope: PiItemScope;
  readonly childThreadId: ThreadId;
  readonly childRootNodeId: NodeId;
  /** Parent thread, which sends the child's task prompt. */
  readonly parentThreadId: ThreadId;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  /** The timestamp of the message being streamed; see `observeChildEvent`. */
  messageOrdinal: number;
  readonly streamItems: Map<string, PiStreamItemState>;
  readonly toolArgs: Map<string, unknown>;
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  /** Last status emitted on the child's root node. */
  status: OrchestrationV2ExecutionNode["status"] | null;
  /** The session this adapter observes, once `observe` succeeded. */
  observedSessionId: string | null;
  /** Sessions `observe` already failed for, so a busy roster does not retry in a loop. */
  readonly failedSessionIds: Set<string>;
}

type PiItemSink = ActivePiTurn | PiChildTranscript;

function itemScope(sink: PiItemSink): PiItemScope {
  if ("scope" in sink) return sink.scope;
  return {
    threadId: sink.turnInput.threadId,
    runId: sink.turnInput.runId,
    rootNodeId: sink.turnInput.rootNodeId,
    providerThreadId: sink.turnInput.providerThread.id,
    providerTurnId: sink.providerTurn.id,
    idPrefix: sink.providerTurn.id,
  };
}

function rlmChildStatus(
  status: string | undefined,
): "pending" | "running" | "completed" | "failed" | "cancelled" {
  switch (status) {
    case "queued":
      return "pending";
    case "done":
      return "completed";
    case "error":
      return "failed";
    case "cancelled":
      return "cancelled";
    default:
      return "running";
  }
}

/** A short live status line: the child's own progress note, else what it is doing. */
function rlmChildProgress(snapshot: unknown): string | undefined {
  const note = recordString(snapshot, "progressNote");
  if (note !== undefined && note.length > 0) return note.slice(0, 200);
  const activity = recordField(snapshot, "activity");
  const kind = recordString(activity, "kind");
  if (kind === undefined) return undefined;
  const toolName = recordString(activity, "toolName");
  return toolName === undefined ? kind : `${kind} ${toolName}`;
}

interface PiThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActivePiTurn | null;
}

// ── adapter ───────────────────────────────────────────────────

export function makePiAdapterV2(
  options: PiAdapterV2Options,
): ProviderAdapter.ProviderAdapterV2Shape {
  const { idAllocator, flavor } = options;
  const driver = flavor.driverKind;
  const name = flavor.displayName;
  const binary = options.settings.binaryPath || flavor.defaultBinary;
  const capabilities = piProviderCapabilities(flavor);
  const unsolicitedActivityError = `${name} started agent work outside an active T3 turn. The session was stopped to prevent invisible tool execution.`;
  const providerRef = (
    nativeId: string,
    strength: "strong" | "weak" = "strong",
  ): OrchestrationV2ProviderRef => ({ driver, nativeId, strength });

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver,
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("PiAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? options.serverConfig.cwd;
      // Prime Agent reports edited paths through realpath (/tmp is /private/tmp
      // on macOS), so compare against both spellings of the workspace.
      const workspaceRoots = [
        cwd,
        yield* options.fileSystem.realPath(cwd).pipe(Effect.orElseSucceed(() => cwd)),
      ].map((root) => root.replace(/\/+$/, ""));
      const workspaceRelativePath = (path: string) => {
        const root = workspaceRoots.find((candidate) => path.startsWith(`${candidate}/`));
        return root === undefined ? path : path.slice(root.length + 1);
      };
      const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
      const provideCacheFs = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem>) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, options.fileSystem),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        );
      // The extension owns both the optional MCP bridge and Pi's permission
      // hook. Materialize it even when this session has no MCP credential so
      // Supervised never silently degrades to unrestricted tool execution.
      const extensionPath = yield* provideCacheFs(
        materializePiT3McpExtension(options.serverConfig.providerStatusCacheDir),
      );
      const resolvedLaunchArgs = resolvePiLaunchArgs(options.settings.launchArgs);
      if (!resolvedLaunchArgs.ok) {
        return yield* protocolError(resolvedLaunchArgs.message);
      }
      const launch = buildPiRpcLaunch({
        launchArgs: resolvedLaunchArgs.args,
        environment: options.environment,
        mcpSession,
        extensionPath,
        runtimeMode: input.runtimePolicy.runtimeMode,
      });
      const connection: PiRpcConnection = yield* makePiRpcConnection({
        command: binary,
        args: launch.args,
        cwd,
        env: launch.env,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, options.spawner),
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      /** Whether T3's extension command for in-place rollback is loaded; null until discovered. */
      let navigateTreeAvailable: boolean | null = null;
      const discoverSkillNames = connection
        .request({ type: "get_commands" }, PI_SKILL_DISCOVERY_TIMEOUT_MS)
        .pipe(
          Effect.map((data) => {
            navigateTreeAvailable = hasPiNavigateTreeCommand(data);
            return new Set(parsePiDiscoveredCommands(data).skills.map((skill) => skill.name));
          }),
        );
      let skillNames: Set<string> | null = null;
      /** In-place rollbacks waiting for the extension command's reported outcome. */
      const pendingTreeNavigations = new Map<string, Deferred.Deferred<unknown>>();
      let treeNavigationCounter = 0;

      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const pendingPrompts = new Map<string, PendingPiPrompt>();
      const sessionApprovals = new Set<string>();
      /**
       * Prime Agent subagents (`rlm.spawn`) by child id. The parent usually
       * ends its run right after spawning, so its turn settles while the child
       * keeps running as background work; the child's reply wakes the parent
       * into a continuation run. Updates stay on the turn that spawned it.
       */
      const rlmChildren = new Map<string, PiRlmChildState>();
      /**
       * The T3 thread of each Prime Agent child, kept after the child ends. Its
       * ids derive from the child id, so a child that wakes again continues
       * the same transcript.
       */
      const rlmTranscripts = new Map<string, PiChildTranscript>();
      /** Observed child sessions by `activeSessionId`, to route their events. */
      const observedTranscripts = new Map<string, PiChildTranscript>();
      const hasLiveRlmChildren = () =>
        Array.from(rlmChildren.values()).some((child) => !child.terminal);
      /**
       * Shell commands a cell left running in the background, by task id. The
       * kernel only reports them when they finish (`async_bash_completion`),
       * so they are listed from the cell that started them until then.
       */
      const backgroundJobs = new Map<string, DetachedBashJob>();
      let backgroundJobCounter = 0;
      const hasPendingBackgroundWork = () =>
        hasLiveRlmChildren() || backgroundJobs.size > 0 || pendingWake !== null;
      // Answering a dialog and terminalizing a turn both publish lifecycle
      // events. Pi can settle immediately after `extension_ui_response`, so
      // serialize the two paths to stop `turn.terminal` from overtaking the
      // dialog's own resolution updates.
      const sessionEventPermit = yield* Semaphore.make(1);
      let threadState: PiThreadState | null = null;
      let registrationAttempted = false;
      let lastNativeThreadId: string | null = null;
      // User Stop intentionally tears down this RPC process after aborting.
      // Keep that intent beyond turn finalization so the later stdout close is
      // not mistaken for an unexpected transport failure.
      let stopRequested = false;
      // Pi extensions can trigger an agent turn after the owning T3 turn has
      // settled. Until orchestration has a first-class provider-initiated run,
      // stop that runtime before it can execute tools without a timeline owner.
      let unsolicitedActivityDetected = false;
      /**
       * Work the agent started on its own (a self-wake), buffered until the
       * continuation run it requested, or the user's next turn, adopts it.
       */
      let pendingWake: PendingPiWake | null = null;
      let wakeGeneration = 0;
      let appliedModel: string | null = null;
      let appliedThinking: string | null = null;
      /** Last thread title synced into pi's session name (`/resume` listing). */
      let appliedSessionName: string | null = null;
      /** Extension failures raised during startup are attached to the next turn. */
      const outOfTurnExtensionErrors: Array<PiRpcRecord> = [];
      /**
       * Leaf entry id of the pi session tree as of the last turn boundary.
       * Turn-start user entries are located relative to it, giving each
       * provider turn a durable native ref for session-tree rollback.
       */
      let lastKnownLeaf: string | null = null;
      /**
       * Set when a `get_entries` capture failed. Pi may have advanced past
       * `lastKnownLeaf` since, so the cursor no longer bounds a single turn
       * and the next capture re-syncs it instead of trusting it.
       */
      let leafCursorStale = false;
      /**
       * Fork-messages flavors: user entry ids already on the active branch as
       * of the last turn boundary. A turn's start entry is the first id that
       * is new at its end. Stale after a failed read, like `lastKnownLeaf`.
       */
      let knownUserEntryIds = new Set<string>();
      // Pi's own configured defaults, captured from the first `get_state` so
      // that selecting the displayed "Pi default" again can restore them. Pi
      // has no "unset" commands, so the baselines have to be replayed
      // explicitly.
      let baselineModel: { provider: string; modelId: string } | null = null;
      let baselineThinking: string | null = null;
      /** Context window of the model Pi currently runs, from get_state and set_model. */
      let contextWindow: number | null = null;
      const modelContextWindows = new Map<string, number>();
      let modelsDiscovered = false;
      // Prompt responses carry no id. Keep their session-wide send order and
      // owner so a late ack from a settled turn cannot affect the next turn.
      const pendingPromptResponses: Array<{
        readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
        readonly kind: "turn_start" | "steer";
      }> = [];
      const pendingCompactResponses: Array<{
        readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
        readonly kind: "turn_start" | "steer";
      }> = [];

      const compactRpcRecord = (command: PiCompactCommand): PiRpcRecord =>
        command.customInstructions === undefined
          ? { type: "compact" }
          : { type: "compact", customInstructions: command.customInstructions };

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession = (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread = (
        state: PiThreadState,
        patch: Partial<OrchestrationV2ProviderThread>,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          state.providerThread = { ...state.providerThread, ...patch, updatedAt };
          yield* emit({
            type: "provider_thread.updated",
            driver,
            providerThread: state.providerThread,
          });
        });

      const itemOrdinal = (turn: PiItemSink, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const request = (record: PiRpcRecord, timeoutMs = PI_REQUEST_TIMEOUT_MS) =>
        connection.request(record, timeoutMs);

      const nonNegativeInteger = (input: unknown, key: string): number | undefined => {
        const value = recordNumber(input, key);
        return value === undefined ? undefined : Math.max(0, Math.trunc(value));
      };

      const rememberModelContextWindow = (model: unknown): number | null => {
        const provider = recordString(model, "provider");
        const id = recordString(model, "id");
        const capacity = nonNegativeInteger(model, "contextWindow");
        if (provider !== undefined && id !== undefined && capacity !== undefined && capacity > 0) {
          modelContextWindows.set(`${provider}/${id}`, capacity);
          return capacity;
        }
        return null;
      };

      const lifecycleRequest = (record: PiRpcRecord) =>
        request(record, PI_SESSION_TIMEOUT_MS).pipe(
          // A local timeout does not cancel Pi's lifecycle hook. Retire the
          // process before fallback can race its eventual switch/new-session.
          Effect.tapError((error) =>
            Effect.logWarning(`${name} session lifecycle request failed`, {
              providerSessionId: input.providerSessionId,
              operation: record["type"],
              errorTag: error._tag,
            }),
          ),
          Effect.catchTags({
            PiRpcTimeoutError: (error) =>
              connection.terminate.pipe(Effect.andThen(Effect.fail(error))),
          }),
          Effect.onInterrupt(() => connection.terminate),
        );

      const tokenUsageFromStats = (
        stats: unknown,
        fallbackUsedTokens: number | null,
        updatedAt: DateTime.Utc,
      ): OrchestrationV2ProviderTurnTokenUsage | undefined => {
        const contextUsage = recordField(stats, "contextUsage");
        const maxTokens = nonNegativeInteger(contextUsage, "contextWindow");
        const usedTokens =
          nonNegativeInteger(contextUsage, "tokens") ?? fallbackUsedTokens ?? undefined;
        if (usedTokens === undefined || maxTokens === undefined || maxTokens === 0)
          return undefined;

        const totals = recordField(stats, "tokens");
        const inputTokens = nonNegativeInteger(totals, "input");
        const cachedInputTokens = nonNegativeInteger(totals, "cacheRead");
        const outputTokens = nonNegativeInteger(totals, "output");
        return {
          usedTokens,
          maxTokens,
          ...(inputTokens === undefined ? {} : { inputTokens }),
          ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
          ...(outputTokens === undefined ? {} : { outputTokens }),
          updatedAt: DateTime.formatIso(updatedAt),
        };
      };

      /**
       * Pi only reports context usage through `get_session_stats`, so the
       * settled turn carries it on the base's per-turn `tokenUsage` (#8144).
       * Usage is secondary telemetry: the request is bounded and a provider
       * version without stats simply leaves the turn without a report, which
       * keeps the meter on the last turn that had one.
       */
      const readTokenUsage = (fallbackUsedTokens: number | null, updatedAt: DateTime.Utc) =>
        request({ type: "get_session_stats" }, 2_000).pipe(
          Effect.map((stats) => tokenUsageFromStats(stats, fallbackUsedTokens, updatedAt)),
          Effect.orElseSucceed(() => undefined),
        );

      /**
       * Pi attaches the current message's cumulative usage to every streaming
       * update (0.84.2+). Emit it on the running turn only when the total
       * changes, so the meter moves live without a burst of no-op updates.
       */
      const reportLiveUsage = (turn: ActivePiTurn, usage: unknown) =>
        Effect.gen(function* () {
          const usedTokens = nonNegativeInteger(usage, "totalTokens");
          if (
            usedTokens === undefined ||
            usedTokens === 0 ||
            usedTokens === turn.lastLiveUsedTokens ||
            contextWindow === null ||
            contextWindow === 0
          ) {
            return;
          }
          turn.lastLiveUsedTokens = usedTokens;
          const inputTokens = nonNegativeInteger(usage, "input");
          const cachedInputTokens = nonNegativeInteger(usage, "cacheRead");
          const outputTokens = nonNegativeInteger(usage, "output");
          const updatedAt = yield* DateTime.now;
          yield* emit({
            type: "provider_turn.updated",
            driver,
            threadId: turn.turnInput.threadId,
            providerTurn: {
              ...turn.providerTurn,
              tokenUsage: {
                usedTokens,
                maxTokens: contextWindow,
                ...(inputTokens === undefined ? {} : { inputTokens }),
                ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
                ...(outputTokens === undefined ? {} : { outputTokens }),
                updatedAt: DateTime.formatIso(updatedAt),
              },
            },
          });
        });

      const baseItemFields = (
        turn: PiItemSink,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => {
        const scope = itemScope(turn);
        return {
          id: idAllocator.derive.turnItemFromProviderItem({
            driver,
            nativeItemId,
          }),
          threadId: scope.threadId,
          runId: scope.runId,
          nodeId: idAllocator.derive.nodeFromProviderItem({
            driver,
            nativeItemId,
          }),
          providerThreadId: scope.providerThreadId,
          providerTurnId: scope.providerTurnId,
          nativeItemRef: providerRef(nativeItemId),
          parentItemId: null,
          ordinal: itemOrdinal(turn, nativeItemId),
          startedAt,
          updatedAt,
        };
      };

      const emitItemNode = (
        turn: PiItemSink,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) => {
        const scope = itemScope(turn);
        return emit({
          type: "node.updated",
          driver,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({
              driver,
              nativeItemId,
            }),
            threadId: scope.threadId,
            runId: scope.runId,
            parentNodeId: scope.rootNodeId,
            rootNodeId: scope.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: scope.providerThreadId,
            providerTurnId: scope.providerTurnId,
            nativeItemRef: providerRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });
      };

      const emitProviderRetry = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        providerRetry: PiProviderRetryState,
        status: "running" | "completed" | "failed" | "interrupted" | "cancelled",
        updatedAt: DateTime.Utc,
      ) {
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: makeProviderRetryTurnItem({
            idAllocator,
            driver,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            nodeId: turn.turnInput.rootNodeId,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            itemOrdinal: providerRetry.itemOrdinal,
            failure: providerRetry.failure,
            retry: providerRetry.retry,
            status,
            startedAt: providerRetry.startedAt,
            updatedAt,
          }),
        });
      });

      const compactionNativeItemId = (turn: ActivePiTurn): string =>
        `compaction:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;

      const emitCompaction = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        compaction: PiCompactionState,
        status: PiCompactionStatus,
        details: {
          readonly summary?: string;
          readonly beforeTokenCount?: number;
          readonly afterTokenCount?: number;
        } = {},
      ) {
        const emittedAt = yield* DateTime.now;
        const completedAt = status === "running" ? null : emittedAt;
        yield* emitItemNode(
          turn,
          compaction.nativeItemId,
          "system",
          status,
          compaction.startedAt,
          completedAt,
        );
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...baseItemFields(turn, compaction.nativeItemId, compaction.startedAt, emittedAt),
            status,
            title: compactionTitle(status),
            completedAt,
            type: "compaction",
            driver,
            ...details,
          },
        });
      });

      // ── streaming text / reasoning ────────────────────────

      const emitStreamItem = (turn: PiItemSink, item: PiStreamItemState, streaming: boolean) =>
        Effect.gen(function* () {
          const scope = itemScope(turn);
          const emittedAt = yield* DateTime.now;
          const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
          yield* emitItemNode(
            turn,
            item.nativeItemId,
            item.kind,
            streaming ? "running" : "completed",
            item.startedAt,
            streaming ? null : emittedAt,
          );
          if (item.kind === "assistant_message") {
            const messageId = idAllocator.derive.messageFromProviderItem({
              driver,
              nativeItemId: item.nativeItemId,
            });
            yield* emit({
              type: "turn_item.updated",
              driver,
              turnItem: {
                ...base,
                status: streaming ? "running" : "completed",
                title: null,
                completedAt: streaming ? null : emittedAt,
                type: "assistant_message",
                messageId,
                text: item.text,
                streaming,
              },
            });
            yield* emit({
              type: "message.updated",
              driver,
              message: {
                id: messageId,
                threadId: scope.threadId,
                runId: scope.runId,
                nodeId: idAllocator.derive.nodeFromProviderItem({
                  driver,
                  nativeItemId: item.nativeItemId,
                }),
                role: "assistant",
                text: item.text,
                attachments: [],
                streaming,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: item.startedAt,
                updatedAt: emittedAt,
              },
            });
            return;
          }
          yield* emit({
            type: "turn_item.updated",
            driver,
            turnItem: {
              ...base,
              status: streaming ? "running" : "completed",
              title: null,
              completedAt: streaming ? null : emittedAt,
              type: "reasoning",
              text: item.text,
              streaming,
            },
          });
        });

      const scheduleStreamFlush = (turn: PiItemSink, item: PiStreamItemState) =>
        Effect.gen(function* () {
          if (item.flushScheduled || item.completed) return;
          item.flushScheduled = true;
          yield* Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                item.flushScheduled = false;
                return item.completed ? Effect.void : emitStreamItem(turn, item, true);
              }),
            ),
            Effect.forkIn(scope),
          );
        });

      const streamItemFor = Effect.fnUntraced(function* (
        turn: PiItemSink,
        kind: PiStreamItemState["kind"],
        contentIndex: number,
      ) {
        const nativeItemId = `${itemScope(turn).idPrefix}:m${turn.messageOrdinal}:c${contentIndex}`;
        const existing = turn.streamItems.get(nativeItemId);
        if (existing !== undefined) return existing;
        const startedAt = yield* DateTime.now;
        const item: PiStreamItemState = {
          nativeItemId,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          startedAt,
        };
        turn.streamItems.set(nativeItemId, item);
        // Ordinal reserved on first delta so items appear in stream order.
        itemOrdinal(turn, nativeItemId);
        return item;
      });

      /**
       * Lossy streams (see `PiFlavor.lossyStream`) can drop `message_start`, so
       * counting starts would give a later message the id of an earlier,
       * completed one. Their messages are keyed by the snapshot's own
       * timestamp instead, on every event. Returns whether the snapshot had one.
       */
      const adoptMessageIdentity = (turn: PiItemSink, message: unknown) => {
        if (!flavor.lossyStream) return false;
        const timestamp = recordNumber(message, "timestamp");
        if (timestamp === undefined) return false;
        turn.messageOrdinal = timestamp;
        return true;
      };

      const completeStreamItem = (turn: PiItemSink, item: PiStreamItemState, text?: string) =>
        Effect.suspend(() => {
          if (item.completed) return Effect.void;
          item.completed = true;
          if (text !== undefined && text.length > 0) item.text = text;
          return item.text.length === 0 ? Effect.void : emitStreamItem(turn, item, false);
        });

      const completeOpenStreamItems = (turn: PiItemSink) =>
        Effect.forEach(
          Array.from(turn.streamItems.values()).filter((item) => !item.completed),
          (item) => completeStreamItem(turn, item),
          { discard: true },
        );

      /**
       * Take the text of every block in a message snapshot as the truth. The
       * lossy streams (see `PiFlavor.lossyStream`) skip deltas, block ends, and
       * whole blocks, but a snapshot is cumulative. A running snapshot keeps
       * its items open; the final message completes them.
       */
      const adoptSnapshot = Effect.fnUntraced(function* (
        turn: PiItemSink,
        message: unknown,
        final: boolean,
      ) {
        const content = recordField(message, "content");
        if (!Array.isArray(content)) return;
        for (let contentIndex = 0; contentIndex < content.length; contentIndex += 1) {
          const block = snapshotBlock(message, contentIndex);
          if (block === undefined) continue;
          const item = yield* streamItemFor(turn, block.kind, contentIndex);
          if (item.completed) continue;
          item.text = block.text;
          if (final) yield* completeStreamItem(turn, item);
          else yield* scheduleStreamFlush(turn, item);
        }
      });

      // ── tools ─────────────────────────────────────────────

      const emitToolItem = Effect.fnUntraced(function* (
        turn: PiItemSink,
        event: PiRpcRecord,
        phase: "start" | "update" | "end",
      ) {
        const toolCallId = recordString(event, "toolCallId");
        const toolName = recordString(event, "toolName") ?? "tool";
        if (toolCallId === undefined) return;
        if (phase === "start") {
          turn.toolArgs.set(toolCallId, event["args"]);
        }
        const args = event["args"] ?? turn.toolArgs.get(toolCallId);
        const emittedAt = yield* DateTime.now;
        const startedAt = turn.toolStartedAt.get(toolCallId) ?? emittedAt;
        turn.toolStartedAt.set(toolCallId, startedAt);
        const completed = phase === "end";
        const resultRecord = completed ? event["result"] : event["partialResult"];
        // Prime Agent reports a failed cell on the result (and its kernel
        // status) while the event-level flag stays false.
        const isError =
          event["isError"] === true ||
          (completed &&
            (recordField(resultRecord, "isError") === true ||
              recordString(recordField(resultRecord, "details"), "status") === "error"));
        const outputText = contentText(recordField(resultRecord, "content"));
        // A Stop aborts in-flight tools, and pi reports those as error ends.
        // Present them as interrupted (matching the run) rather than failed.
        const status = completed
          ? isError
            ? "interrupted" in turn && turn.interrupted
              ? "interrupted"
              : "failed"
            : "completed"
          : "running";
        const base = baseItemFields(turn, toolCallId, startedAt, emittedAt);
        yield* emitItemNode(
          turn,
          toolCallId,
          "tool_call",
          status,
          startedAt,
          completed ? emittedAt : null,
        );
        const shared = {
          ...base,
          status,
          completedAt: completed ? emittedAt : null,
        } as const;
        if (flavor.tools === "ipython" && toolName === "ipython") {
          const cell = classifyIpythonCell(recordString(args, "code") ?? "");
          yield* emit({
            type: "turn_item.updated",
            driver,
            turnItem:
              cell.kind === "bash"
                ? {
                    ...shared,
                    title: "bash",
                    type: "command_execution",
                    input: cell.command,
                    ...(outputText.length > 0 ? { output: outputText } : {}),
                  }
                : {
                    ...shared,
                    title: pythonCellTitle(cell.code),
                    type: "dynamic_tool",
                    toolName: "python",
                    input: { code: cell.code },
                    ...(outputText.length > 0 ? { output: outputText } : {}),
                  },
          });
          if (completed) yield* emitIpythonFileChanges(turn, toolCallId, resultRecord, emittedAt);
          // A child's cells run in the child's kernel, not the parent's.
          if (completed && !isError && "turnInput" in turn) {
            yield* trackBackgroundJobs(recordString(args, "code") ?? "");
          }
          return;
        }
        if (toolName === "bash") {
          const exitCode = recordNumber(recordField(resultRecord, "details"), "exitCode");
          yield* emit({
            type: "turn_item.updated",
            driver,
            turnItem: {
              ...shared,
              title: toolName,
              type: "command_execution",
              input: recordString(args, "command") ?? "",
              ...(outputText.length > 0 ? { output: outputText } : {}),
              ...(exitCode === undefined ? {} : { exitCode }),
            },
          });
          return;
        }
        if (toolName === "edit" || toolName === "write") {
          const fileName = recordString(args, "path") ?? recordString(args, "file_path");
          if (fileName !== undefined) {
            yield* emit({
              type: "turn_item.updated",
              driver,
              turnItem: {
                ...shared,
                title: toolName,
                type: "file_change",
                fileName,
              },
            });
            return;
          }
        }
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...shared,
            title: toolName,
            type: "dynamic_tool",
            toolName,
            input: args ?? {},
            ...(outputText.length > 0 ? { output: outputText } : {}),
          },
        });
        if (toolName === "subagent" && "turnInput" in turn) {
          yield* emitSubagentTasks(turn, toolCallId, resultRecord, completed);
        }
      });

      /**
       * Prime Agent's edit helper reports each change on the cell result as
       * `details.diffs: [{ path, oldStr, newStr }]`. Surface them as file
       * changes under the cell so the diff view works like other providers'.
       */
      const emitIpythonFileChanges = Effect.fnUntraced(function* (
        turn: PiItemSink,
        toolCallId: string,
        resultRecord: unknown,
        emittedAt: DateTime.Utc,
      ) {
        const diffs = recordField(recordField(resultRecord, "details"), "diffs");
        if (!Array.isArray(diffs)) return;
        const startedAt = turn.toolStartedAt.get(toolCallId) ?? emittedAt;
        for (const [index, diff] of diffs.entries()) {
          const reportedPath = recordString(diff, "path")?.trim();
          const fileName =
            reportedPath === undefined ? undefined : workspaceRelativePath(reportedPath);
          const oldStr = recordString(diff, "oldStr");
          const newStr = recordString(diff, "newStr");
          if (fileName === undefined || fileName.length === 0) continue;
          if (oldStr === undefined || newStr === undefined) continue;
          const nativeItemId = `${toolCallId}:diff:${index}`;
          yield* emitItemNode(turn, nativeItemId, "tool_call", "completed", startedAt, emittedAt);
          yield* emit({
            type: "turn_item.updated",
            driver,
            turnItem: {
              ...baseItemFields(turn, nativeItemId, startedAt, emittedAt),
              status: "completed",
              completedAt: emittedAt,
              title: "edit",
              type: "file_change",
              fileName,
              oldStr,
              newStr,
            },
          });
        }
      });

      // ── child threads ─────────────────────────────────────

      const openChildTranscript = Effect.fnUntraced(function* (input: {
        readonly turn: ActivePiTurn;
        readonly childId: string;
        readonly subagentId: NodeId;
        readonly snapshot: unknown;
        readonly title: string | null;
        readonly prompt: string;
        readonly now: DateTime.Utc;
      }) {
        const { turnInput } = input.turn;
        const childThreadId = idAllocator.derive.threadFromProviderThread({
          driver,
          nativeThreadId: `${turnInput.providerThread.id}:rlm:${input.childId}`,
        });
        const childRootNodeId = idAllocator.derive.nodeFromProviderItem({
          driver,
          nativeItemId: `rlm:${input.childId}:thread-root`,
        });
        const transcript: PiChildTranscript = {
          scope: {
            threadId: childThreadId,
            runId: null,
            rootNodeId: childRootNodeId,
            providerThreadId: null,
            providerTurnId: null,
            idPrefix: `rlm:${input.childId}`,
          },
          childThreadId,
          childRootNodeId,
          parentThreadId: turnInput.threadId,
          itemOrdinals: new Map(),
          nextItemOrdinal: 0,
          messageOrdinal: 0,
          streamItems: new Map(),
          toolArgs: new Map(),
          toolStartedAt: new Map(),
          status: null,
          observedSessionId: null,
          failedSessionIds: new Set(),
        };
        rlmTranscripts.set(input.childId, transcript);
        const model = recordString(input.snapshot, "model");
        yield* emit({
          type: "app_thread.created",
          driver,
          appThread: makeSubagentChildThread({
            parentThread: turnInput.appThread,
            childThreadId,
            parentNodeId: input.subagentId,
            activeProviderThreadId: null,
            providerInstanceId: options.instanceId,
            modelSelection:
              model !== undefined && model !== turnInput.modelSelection.model
                ? { instanceId: options.instanceId, model }
                : turnInput.modelSelection,
            title: subagentThreadTitle({
              parentTitle: turnInput.appThread.title,
              prompt: input.prompt,
              title: input.title,
              ordinal: rlmTranscripts.size,
            }),
            now: input.now,
            createdBy: "agent",
            creationSource: "provider",
          }),
        });
        return transcript;
      });

      /** The child thread's root turn carries the child's status for clients. */
      const emitChildRootNode = (
        transcript: PiChildTranscript,
        status: OrchestrationV2ExecutionNode["status"],
        nativeTaskId: string,
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) =>
        Effect.suspend(() => {
          if (transcript.status === status) return Effect.void;
          transcript.status = status;
          return emit({
            type: "node.updated",
            driver,
            node: {
              id: transcript.childRootNodeId,
              threadId: transcript.childThreadId,
              runId: null,
              parentNodeId: null,
              rootNodeId: transcript.childRootNodeId,
              kind: "root_turn",
              status,
              countsForRun: false,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: providerRef(nativeTaskId),
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt,
              completedAt,
            },
          });
        });

      /** A user turn in a child: its task from the parent, or a later agent message. */
      const emitChildUserMessage = Effect.fnUntraced(function* (
        transcript: PiChildTranscript,
        message: unknown,
      ) {
        const text = contentText(recordField(message, "content")).trim();
        if (text.length === 0) return;
        const timestamp = recordNumber(message, "timestamp");
        // Timestamps keep a replayed history and the live stream from
        // emitting the same message twice.
        const nativeItemId = `${transcript.scope.idPrefix}:u${timestamp ?? transcript.nextItemOrdinal}`;
        const now = yield* DateTime.now;
        const artifacts = makeSubagentConversationArtifacts({
          messageId: idAllocator.derive.messageFromProviderItem({ driver, nativeItemId }),
          senderThreadId: transcript.parentThreadId,
          turnItemId: idAllocator.derive.turnItemFromProviderItem({ driver, nativeItemId }),
          threadId: transcript.childThreadId,
          rootNodeId: transcript.childRootNodeId,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: providerRef(nativeItemId),
          role: "user",
          text,
          ordinal: itemOrdinal(transcript, nativeItemId),
          now: Option.getOrElse(DateTime.make(timestamp ?? Number.NaN), () => now),
        });
        yield* emit({ type: "message.updated", driver, message: artifacts.message });
        yield* emit({ type: "turn_item.updated", driver, turnItem: artifacts.turnItem });
      });

      /**
       * Routes one event of an observed child session onto its thread. The
       * child's RPC drops events like the main one, so assistant text comes
       * from message snapshots, and each message is keyed by its own timestamp:
       * a lost `message_start` cannot shift ids, and replaying history over
       * live events is idempotent.
       */
      const observeChildEvent = Effect.fnUntraced(function* (
        transcript: PiChildTranscript,
        event: PiRpcRecord,
      ) {
        switch (event["type"]) {
          case "message_update":
          case "message_end": {
            const message = event["message"];
            const role = recordString(message, "role");
            if (role === "assistant") {
              transcript.messageOrdinal =
                recordNumber(message, "timestamp") ?? transcript.messageOrdinal + 1;
              const final = event["type"] === "message_end";
              yield* adoptSnapshot(transcript, message, final);
              if (final) yield* completeOpenStreamItems(transcript);
              return;
            }
            if (event["type"] === "message_end" && role !== "toolResult") {
              // Custom messages show only when the agent marks them for display.
              if (role === "user" || recordField(message, "display") === true) {
                yield* emitChildUserMessage(transcript, message);
              }
            }
            return;
          }
          case "tool_execution_start":
            yield* emitToolItem(transcript, event, "start");
            return;
          case "tool_execution_update":
            yield* emitToolItem(transcript, event, "update");
            return;
          case "tool_execution_end":
            yield* emitToolItem(transcript, event, "end");
            return;
          default:
            return;
        }
      });

      /** Replays `observe`'s history as the events that would have produced it. */
      const replayChildHistory = Effect.fnUntraced(function* (
        transcript: PiChildTranscript,
        messages: ReadonlyArray<unknown>,
      ) {
        for (const message of messages) {
          const role = recordString(message, "role");
          if (role === "toolResult") {
            yield* observeChildEvent(transcript, {
              type: "tool_execution_end",
              toolCallId: recordString(message, "toolCallId"),
              toolName: recordString(message, "toolName"),
              result: {
                content: recordField(message, "content"),
                details: recordField(message, "details"),
              },
              isError: recordField(message, "isError") === true,
            });
            continue;
          }
          yield* observeChildEvent(transcript, { type: "message_end", message });
          const content = role === "assistant" ? recordField(message, "content") : undefined;
          for (const block of Array.isArray(content) ? content : []) {
            if (recordField(block, "type") !== "toolCall") continue;
            yield* observeChildEvent(transcript, {
              type: "tool_execution_start",
              toolCallId: recordString(block, "id"),
              toolName: recordString(block, "name"),
              args: recordField(block, "arguments"),
            });
          }
        }
      });

      const stopObservingChild = Effect.fnUntraced(function* (
        transcript: PiChildTranscript,
        sessionIsAlive: boolean,
      ) {
        const sessionId = transcript.observedSessionId;
        if (sessionId === null) return;
        transcript.observedSessionId = null;
        observedTranscripts.delete(sessionId);
        if (sessionIsAlive) {
          yield* request({ type: "unobserve", activeSessionId: sessionId }).pipe(Effect.ignore);
        }
      });

      /**
       * Streams a running child's session into its thread: one observation per
       * live child, started when its session id first shows up and dropped once
       * it ends. A failed `observe` is not retried for the same session.
       */
      const syncChildObservation = Effect.fnUntraced(function* (
        transcript: PiChildTranscript,
        snapshot: unknown,
        terminal: boolean,
        sessionIsAlive: boolean,
      ) {
        if (terminal) {
          yield* stopObservingChild(transcript, sessionIsAlive);
          return;
        }
        const sessionId = recordString(snapshot, "activeSessionId");
        if (
          sessionId === undefined ||
          transcript.observedSessionId === sessionId ||
          transcript.failedSessionIds.has(sessionId)
        ) {
          return;
        }
        yield* stopObservingChild(transcript, true);
        const observed = yield* request({ type: "observe", activeSessionId: sessionId }).pipe(
          Effect.option,
        );
        if (Option.isNone(observed)) {
          transcript.failedSessionIds.add(sessionId);
          yield* Effect.logWarning(`${name} could not observe a child session`, { sessionId });
          return;
        }
        transcript.observedSessionId = sessionId;
        observedTranscripts.set(sessionId, transcript);
        const history = recordField(observed.value, "messages");
        yield* replayChildHistory(transcript, Array.isArray(history) ? history : []);
      });

      /**
       * Observe the result shape from Pi's official example subagent extension.
       * The extension runs children with --no-session, so these entries are
       * visible in T3's shared subagent UI without inventing a child thread.
       * Unknown or changed result shapes stay ordinary dynamic tool output.
       */
      /** Mirrors one `rlm_child_update` roster snapshot onto T3's subagent surfaces. */
      const emitRlmChild = Effect.fnUntraced(function* (
        snapshot: unknown,
        currentTurn: ActivePiTurn | null,
        statusOverride?: "interrupted",
      ) {
        const childId = recordString(snapshot, "id");
        if (childId === undefined) return;
        const emittedAt = yield* DateTime.now;
        const previous = rlmChildren.get(childId);
        // Nested children report through the root session with their parent
        // child's id. Their first update can arrive after the root turn settled.
        const parentChildId = recordString(snapshot, "parentId");
        const parentChild =
          parentChildId === undefined ? undefined : rlmChildren.get(parentChildId);
        // A child outlives the run that spawned it; its card stays on that run.
        const turn = previous?.turn ?? parentChild?.turn ?? currentTurn;
        if (turn === null) return;
        const status = statusOverride ?? rlmChildStatus(recordString(snapshot, "status"));
        const terminal = status !== "pending" && status !== "running";
        // A child that finished in an earlier turn already shows its outcome.
        // Later roster churn (the parent deleting it, a resynced roster) must
        // not rewrite that card from an unrelated turn.
        if (previous === undefined && terminal) return;
        const state: PiRlmChildState = {
          snapshot,
          startedAt: previous?.startedAt ?? emittedAt,
          terminal,
          turn,
        };
        rlmChildren.set(childId, state);
        const nativeTaskId = `rlm:${childId}`;
        const subagentId = idAllocator.derive.nodeFromProviderItem({
          driver,
          nativeItemId: nativeTaskId,
        });
        const title = recordString(snapshot, "sessionName") ?? null;
        const prompt = recordString(snapshot, "label") ?? title ?? "child agent";
        const progress = terminal ? undefined : rlmChildProgress(snapshot);
        const result = terminal
          ? (recordString(snapshot, "error") ?? recordString(snapshot, "answerPreview") ?? null)
          : null;
        const completedAt = terminal ? emittedAt : null;
        const transcript = flavor.childThreads
          ? (rlmTranscripts.get(childId) ??
            (yield* openChildTranscript({
              turn,
              childId,
              subagentId,
              snapshot,
              title,
              prompt,
              now: emittedAt,
            })))
          : undefined;
        if (transcript !== undefined) {
          yield* emitChildRootNode(transcript, status, nativeTaskId, state.startedAt, completedAt);
        }
        yield* emit({
          type: "subagent.updated",
          driver,
          subagent: {
            id: subagentId,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            // Nesting under the parent subagent's node is what lets clients
            // draw Prime Agent's subagent tree.
            parentNodeId:
              parentChildId === undefined
                ? turn.turnInput.rootNodeId
                : idAllocator.derive.nodeFromProviderItem({
                    driver,
                    nativeItemId: `rlm:${parentChildId}`,
                  }),
            origin: "provider_native",
            createdBy: "agent",
            driver,
            providerInstanceId: options.instanceId,
            providerThreadId: turn.turnInput.providerThread.id,
            childThreadId: transcript?.childThreadId ?? null,
            nativeTaskRef: providerRef(nativeTaskId),
            prompt,
            title,
            model: recordString(snapshot, "model") ?? null,
            status,
            ...(progress === undefined ? {} : { progress }),
            result,
            startedAt: state.startedAt,
            completedAt,
            updatedAt: emittedAt,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...baseItemFields(turn, nativeTaskId, state.startedAt, emittedAt),
            status,
            title,
            completedAt,
            type: "subagent",
            subagentId,
            origin: "provider_native",
            driver,
            providerInstanceId: options.instanceId,
            childThreadId: transcript?.childThreadId ?? null,
            prompt,
            ...(progress === undefined ? {} : { progress }),
            result,
          },
        });
        if (transcript !== undefined) {
          yield* syncChildObservation(transcript, snapshot, terminal, statusOverride === undefined);
        }
        if (terminal) rlmChildren.delete(childId);
      });

      const publishBackgroundJobs = Effect.fnUntraced(function* () {
        const state = threadState;
        if (state === null) return;
        yield* updateProviderThread(state, {
          pendingBackgroundTasks: Array.from(backgroundJobs, ([taskId, job]) => ({
            taskId,
            kind: "command" as const,
            description: job.command,
          })),
        });
      });

      /** Tracks jobs a finished cell started in the background, or consumed by awaiting. */
      const trackBackgroundJobs = Effect.fnUntraced(function* (code: string) {
        let changed = false;
        for (const [taskId, job] of backgroundJobs) {
          if (job.variable !== null && awaitsHandle(code, job.variable)) {
            backgroundJobs.delete(taskId);
            changed = true;
          }
        }
        for (const job of detachedBashJobs(code)) {
          backgroundJobs.set(`bash:${++backgroundJobCounter}`, job);
          changed = true;
        }
        if (changed) yield* publishBackgroundJobs();
      });

      /** The kernel reports a detached job's end with an `async_bash_completion` message. */
      const completeBackgroundJob = Effect.fnUntraced(function* (message: unknown) {
        if (recordString(message, "customType") !== "async_bash_completion") return;
        const reported = recordString(recordField(message, "details"), "command");
        if (reported === undefined) return;
        for (const [taskId, job] of backgroundJobs) {
          if (reportedCommandMatches(job, reported)) {
            backgroundJobs.delete(taskId);
            yield* publishBackgroundJobs();
            return;
          }
        }
      });

      const emitSubagentTasks = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        toolCallId: string,
        resultRecord: unknown,
        completed: boolean,
      ) {
        const results = recordField(recordField(resultRecord, "details"), "results");
        if (!Array.isArray(results)) return;
        const emittedAt = yield* DateTime.now;
        const parentNodeId = idAllocator.derive.nodeFromProviderItem({
          driver,
          nativeItemId: toolCallId,
        });
        for (const [index, result] of results.entries()) {
          const agent = recordString(result, "agent");
          const task = recordString(result, "task");
          if (agent === undefined || task === undefined) continue;
          const nativeTaskId = `${toolCallId}:subagent:${recordNumber(result, "step") ?? index}`;
          const subagentId = idAllocator.derive.nodeFromProviderItem({
            driver,
            nativeItemId: nativeTaskId,
          });
          const startedAt = turn.toolStartedAt.get(nativeTaskId) ?? emittedAt;
          turn.toolStartedAt.set(nativeTaskId, startedAt);
          const finished = completed || recordField(result, "finished") === true;
          const stopReason = recordString(result, "stopReason");
          const interrupted = finished && stopReason === "aborted";
          const failed =
            finished &&
            !interrupted &&
            ((recordNumber(result, "exitCode") ?? 0) !== 0 || stopReason === "error");
          const status = interrupted
            ? "interrupted"
            : failed
              ? "failed"
              : finished
                ? "completed"
                : "running";
          const outputText = piSubagentOutput(result);
          const progress =
            !finished && outputText.length > 0 ? { progress: outputText.slice(0, 200) } : {};
          const resultText = finished && outputText.length > 0 ? outputText.slice(0, 10_000) : null;
          yield* emit({
            type: "subagent.updated",
            driver,
            subagent: {
              id: subagentId,
              threadId: turn.turnInput.threadId,
              runId: turn.turnInput.runId,
              parentNodeId,
              origin: "provider_native",
              createdBy: "agent",
              driver,
              providerInstanceId: options.instanceId,
              providerThreadId: turn.turnInput.providerThread.id,
              childThreadId: null,
              nativeTaskRef: providerRef(nativeTaskId),
              prompt: task,
              title: agent,
              model: recordString(result, "model") ?? null,
              status,
              ...progress,
              result: resultText,
              startedAt,
              completedAt: finished ? emittedAt : null,
              updatedAt: emittedAt,
            },
          });
          yield* emit({
            type: "turn_item.updated",
            driver,
            turnItem: {
              ...baseItemFields(turn, nativeTaskId, startedAt, emittedAt),
              status,
              title: agent,
              completedAt: finished ? emittedAt : null,
              type: "subagent",
              subagentId,
              origin: "provider_native",
              driver,
              providerInstanceId: options.instanceId,
              childThreadId: null,
              prompt: task,
              ...progress,
              result: resultText,
            },
          });
        }
      });

      // ── extension UI prompts ──────────────────────────────

      const cancelPrompt = (pending: PendingPiPrompt, resolvedAt: DateTime.Utc) =>
        Effect.gen(function* () {
          yield* connection
            .send({
              type: "extension_ui_response",
              id: pending.nativeRequestId,
              cancelled: true,
            })
            .pipe(Effect.ignore);
          pending.runtimeRequest = {
            ...pending.runtimeRequest,
            status: "cancelled",
            resolvedAt,
          };
          yield* emit({
            type: "runtime_request.updated",
            driver,
            threadId: pending.node.threadId,
            runtimeRequest: pending.runtimeRequest,
          });
          yield* emit({
            type: "node.updated",
            driver,
            node: { ...pending.node, status: "cancelled", completedAt: resolvedAt },
          });
          yield* emit({
            type: "turn_item.updated",
            driver,
            turnItem: {
              ...pending.turnItem,
              status: "cancelled",
              completedAt: resolvedAt,
              updatedAt: resolvedAt,
            },
          });
        });

      const cancelPendingPrompts = (resolvedAt: DateTime.Utc) =>
        Effect.gen(function* () {
          const pending = Array.from(pendingPrompts.values());
          pendingPrompts.clear();
          yield* Effect.forEach(pending, (prompt) => cancelPrompt(prompt, resolvedAt), {
            discard: true,
          });
        });

      const handleExtensionUiRequest = Effect.fnUntraced(function* (event: PiRpcRecord) {
        const method = recordString(event, "method");
        const nativeRequestId = recordString(event, "id");
        if (method === undefined) return;
        const notifyMessage = method === "notify" ? recordString(event, "message") : undefined;
        if (notifyMessage?.startsWith(T3_NAVIGATE_TREE_RESULT_MARKER) === true) {
          const result = parseNavigateTreeResult(
            notifyMessage.slice(T3_NAVIGATE_TREE_RESULT_MARKER.length),
          );
          const pending = pendingTreeNavigations.get(recordString(result, "requestId") ?? "");
          if (pending !== undefined) yield* Deferred.succeed(pending, result);
          return;
        }
        if (method === "notify") {
          const state = threadState;
          const turn = state?.activeTurn ?? null;
          const message = recordString(event, "message") ?? "";
          if (turn === null || message.length === 0) return;
          const emittedAt = yield* DateTime.now;
          const nativeItemId = `notify:${turn.nextItemOrdinal}`;
          yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
          yield* emit({
            type: "turn_item.updated",
            driver,
            turnItem: {
              ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
              status: "completed",
              completedAt: emittedAt,
              title: "notify",
              type: "dynamic_tool",
              toolName: "notify",
              input: {
                message,
                notifyType: recordString(event, "notifyType") ?? "info",
              },
            },
          });
          return;
        }
        if (
          method !== "select" &&
          method !== "confirm" &&
          method !== "input" &&
          method !== "editor"
        ) {
          // Terminal decoration has no matching T3 surface.
          yield* Effect.logDebug("Ignoring pi extension UI update.", { method });
          return;
        }
        if (nativeRequestId === undefined) return;
        const approvalTitle = recordString(event, "title") ?? "";
        const approvalKey = `${approvalTitle.length}:${approvalTitle}${recordString(event, "message") ?? ""}`;
        if (method === "confirm" && sessionApprovals.has(approvalKey)) {
          yield* connection.send({
            type: "extension_ui_response",
            id: nativeRequestId,
            confirmed: true,
          });
          return;
        }
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        const createdAt = yield* DateTime.now;
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver,
          ...(turn === null ? {} : { providerTurnId: turn.providerTurn.id }),
          nativeRequestId,
        });
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const title = recordString(event, "title") ?? method;
        const threadId =
          turn?.turnInput.threadId ?? state?.providerThread.appThreadId ?? input.threadId;
        const providerThreadId = state?.providerThread.id ?? null;
        const providerTurnId = turn?.providerTurn.id ?? null;
        const runtimeRequest: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId,
          nativeRequestRef: providerRef(nativeRequestId),
          kind: method === "confirm" ? "command" : "user_input",
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt,
          resolvedAt: null,
        };
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId,
          runId: turn?.turnInput.runId ?? null,
          parentNodeId: turn?.turnInput.rootNodeId ?? null,
          rootNodeId: turn?.turnInput.rootNodeId ?? nodeId,
          kind: method === "confirm" ? "approval_request" : "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId,
          providerTurnId,
          nativeItemRef: providerRef(nativeRequestId),
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: createdAt,
          completedAt: null,
        };
        const itemBase = {
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          threadId,
          runId: turn?.turnInput.runId ?? null,
          nodeId,
          providerThreadId,
          providerTurnId,
          nativeItemRef: providerRef(nativeRequestId),
          parentItemId: null,
          // Runless startup/session-switch requests are normalized into the
          // thread-level ordinal range by TurnItemPositionStore.
          ordinal: turn === null ? 0 : itemOrdinal(turn, nativeRequestId),
          status: "waiting" as const,
          title,
          startedAt: createdAt,
          completedAt: null,
          updatedAt: createdAt,
        };
        const turnItem: OrchestrationV2TurnItem =
          method === "confirm"
            ? {
                ...itemBase,
                type: "approval_request",
                requestId,
                requestKind: piApprovalRequestKind(title),
                prompt: recordString(event, "message") ?? title,
              }
            : {
                ...itemBase,
                type: "user_input_request",
                requestId,
                questions: [piQuestion(nativeRequestId, method, title, event)],
              };
        pendingPrompts.set(String(requestId), {
          nativeRequestId,
          method,
          questionId: nativeRequestId,
          approvalKey,
          runtimeRequest,
          node,
          turnItem,
        });
        yield* emit({
          type: "runtime_request.updated",
          driver,
          threadId,
          runtimeRequest,
        });
        yield* emit({ type: "node.updated", driver, node });
        yield* emit({ type: "turn_item.updated", driver, turnItem });
      });

      const emitExtensionError = Effect.fnUntraced(function* (event: PiRpcRecord) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (turn === null) {
          outOfTurnExtensionErrors.push(event);
          return;
        }
        const emittedAt = yield* DateTime.now;
        const nativeItemId = `extension-error:${turn.nextItemOrdinal}`;
        const extensionName = piExtensionDisplayName(recordString(event, "extensionPath"));
        const extensionEvent = recordString(event, "event");
        const detail = recordString(event, "error")?.trim();
        const message = [
          `${extensionName} failed${extensionEvent === undefined ? "" : ` during ${extensionEvent}`}.`,
          detail === undefined || detail.length === 0 ? undefined : detail.slice(0, 2_000),
        ]
          .filter((part): part is string => part !== undefined)
          .join("\n\n");
        const failure = makeProviderFailure({
          message,
          class: "provider_error",
          retryable: false,
        });
        yield* emitItemNode(turn, nativeItemId, "system", "failed", emittedAt, emittedAt);
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "failed",
            title: extensionName,
            completedAt: emittedAt,
            type: "error",
            failure,
          },
        });
      });

      /**
       * Notice ids are derived from the provider item id alone, so they need
       * the provider turn id: item ordinals restart at the same value in every
       * thread and attempt.
       */
      const nextNoticeId = (turn: ActivePiTurn, kind: string, key?: string) => {
        turn.noticeCount += 1;
        return `${turn.providerTurn.id}:${kind}:${key ?? turn.noticeCount}`;
      };

      const emitNotice = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        nativeItemId: string,
        message: string,
        status: "running" | "completed",
        startedAt: DateTime.Utc,
        emittedAt: DateTime.Utc,
      ) {
        const completedAt = status === "completed" ? emittedAt : null;
        yield* emitItemNode(turn, nativeItemId, "system", status, startedAt, completedAt);
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, startedAt, emittedAt),
            status,
            title: message,
            completedAt,
            type: "system_notice",
            message,
          },
        });
      });

      /**
       * Prime Agent runs a model call for its own harness review after the
       * final reply and before `agent_end`, and says nothing while it does.
       * Without a row the turn looks like it is still thinking. Most turns
       * skip the review, so the row waits a moment before it appears.
       */
      const openFinishingUp = Effect.fnUntraced(function* (turn: ActivePiTurn) {
        if (flavor.settleSignal !== "idle_probe" || turn.finishingUp !== null || turn.interrupted) {
          return;
        }
        const startedAt = yield* DateTime.now;
        const finishingUp = {
          nativeItemId: nextNoticeId(turn, "finishing-up"),
          startedAt,
          shown: false,
        };
        turn.finishingUp = finishingUp;
        yield* Effect.sleep(FINISHING_UP_DELAY).pipe(
          Effect.andThen(
            sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                if (turn.finishingUp !== finishingUp || threadState?.activeTurn !== turn) return;
                finishingUp.shown = true;
                const shownAt = yield* DateTime.now;
                yield* emitNotice(
                  turn,
                  finishingUp.nativeItemId,
                  FINISHING_UP_LABEL,
                  "running",
                  startedAt,
                  shownAt,
                );
              }),
            ),
          ),
          Effect.forkIn(scope),
        );
      });

      const closeFinishingUp = Effect.fnUntraced(function* (turn: ActivePiTurn) {
        const finishingUp = turn.finishingUp;
        if (finishingUp === null) return;
        turn.finishingUp = null;
        if (!finishingUp.shown) return;
        const completedAt = yield* DateTime.now;
        yield* emitNotice(
          turn,
          finishingUp.nativeItemId,
          FINISHED_UP_LABEL,
          "completed",
          finishingUp.startedAt,
          completedAt,
        );
      });

      const emitRefineOutcome = Effect.fnUntraced(function* (event: PiRpcRecord) {
        const turn = threadState?.activeTurn ?? null;
        if (turn === null || flavor.settleSignal !== "idle_probe") return;
        const emittedAt = yield* DateTime.now;
        if (event["type"] === "refine_complete") {
          const result = recordField(event, "result");
          const summary = recordString(result, "summary")?.trim();
          const nativeItemId = nextNoticeId(turn, "refine", recordString(result, "id"));
          const message =
            summary === undefined || summary.length === 0
              ? "Refined its harness."
              : `Refined its harness: ${summary}`;
          yield* emitNotice(turn, nativeItemId, message, "completed", emittedAt, emittedAt);
          return;
        }
        const detail = recordString(event, "error")?.trim();
        const message =
          detail === undefined || detail.length === 0
            ? "Harness refinement failed."
            : `Harness refinement failed: ${detail.slice(0, 500)}`;
        yield* emitNotice(
          turn,
          nextNoticeId(turn, "refine-failed"),
          message,
          "completed",
          emittedAt,
          emittedAt,
        );
      });

      // ── turn lifecycle ────────────────────────────────────

      /**
       * Locate this turn's first user entry and the new leaf in pi's session
       * tree. The user-entry id becomes the provider turn's native ref (the
       * point `fork` rolls back to); the leaf becomes the conversation head.
       * Pure bookkeeping: failures degrade to the synthetic refs.
       */
      /** Active-branch user entry ids from `get_fork_messages`, in branch order. */
      const readUserEntryIds = (timeoutMs = PI_REQUEST_TIMEOUT_MS) =>
        request({ type: "get_fork_messages" }, timeoutMs).pipe(
          Effect.map((data) => {
            const messages = recordField(data, "messages");
            return Array.isArray(messages)
              ? messages.flatMap((message) => recordString(message, "entryId") ?? [])
              : [];
          }),
          Effect.option,
        );

      /**
       * Record where the next turn starts. Pi baselines the tree leaf;
       * fork-messages flavors baseline the user entries already present.
       */
      const baselineSessionTree = Effect.fnUntraced(function* () {
        if (flavor.sessionTree === "fork_messages") {
          const entryIds = yield* readUserEntryIds();
          knownUserEntryIds = new Set(Option.getOrElse(entryIds, () => []));
          leafCursorStale = Option.isNone(entryIds);
          return null;
        }
        const entriesData = yield* request({ type: "get_entries" }).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        lastKnownLeaf = recordString(entriesData, "leafId") ?? null;
        // A successful full baseline makes the cursor trustworthy again. Only
        // a failed one leaves it stale, so a recovered session does not keep
        // skipping turn refs. An empty tree is a success with no leafId.
        leafCursorStale = entriesData === undefined;
        return lastKnownLeaf;
      });

      /**
       * Fork-messages flavors cannot see the leaf, so they only recover the
       * turn's start entry; rollback and fork need nothing else.
       */
      const captureTurnStartFromUserEntries = Effect.fnUntraced(function* (
        timeoutMs: number,
      ): Effect.fn.Return<PiTurnTreeRefs | null> {
        const cursorWasStale = leafCursorStale;
        const entryIds = yield* readUserEntryIds(timeoutMs);
        if (Option.isNone(entryIds)) {
          leafCursorStale = true;
          return null;
        }
        const firstNewEntryId = cursorWasStale
          ? undefined
          : entryIds.value.find((entryId) => !knownUserEntryIds.has(entryId));
        knownUserEntryIds = new Set(entryIds.value);
        leafCursorStale = false;
        return { turnStartEntryId: firstNewEntryId ?? null, leafId: null };
      });

      const captureTurnTreeRefs = Effect.fnUntraced(function* (
        timeoutMs = PI_REQUEST_TIMEOUT_MS,
      ): Effect.fn.Return<PiTurnTreeRefs | null> {
        if (flavor.sessionTree === "fork_messages") {
          return yield* captureTurnStartFromUserEntries(timeoutMs);
        }
        const cursorWasStale = leafCursorStale;
        const cursor = cursorWasStale ? null : lastKnownLeaf;
        const data = yield* request(
          {
            type: "get_entries",
            ...(cursor === null ? {} : { since: cursor }),
          },
          timeoutMs,
        ).pipe(Effect.orElseSucceed(() => undefined));
        if (data === undefined) {
          // Pi may have advanced past `lastKnownLeaf` while this failed, so the
          // cursor can no longer be trusted to bound a single turn.
          leafCursorStale = true;
          return null;
        }
        const entries = recordField(data, "entries");
        const leafId = recordString(data, "leafId");
        if (leafId !== undefined) lastKnownLeaf = leafId;
        // Without a trustworthy cursor this window spans more than one turn, so
        // its first user entry belongs to an earlier turn. Re-sync the cursor
        // and skip the turn-start ref rather than pointing rollback too far
        // back; the next turn gets an accurate ref again.
        leafCursorStale = false;
        const firstUserEntryId = cursorWasStale
          ? undefined
          : Array.isArray(entries)
            ? entries
                .filter(
                  (entry) =>
                    recordField(entry, "type") === "message" &&
                    recordString(recordField(entry, "message"), "role") === "user",
                )
                .map((entry) => recordString(entry, "id"))
                .find((id) => id !== undefined)
            : undefined;
        return {
          turnStartEntryId: firstUserEntryId ?? null,
          leafId: leafId ?? null,
        };
      });

      const recordTurnStartEntry = Effect.fnUntraced(function* (turn: ActivePiTurn) {
        // Keep the read short: it holds the event stream mid-reply.
        const refs = yield* captureTurnTreeRefs(2_000);
        turn.startEntryId = refs?.turnStartEntryId ?? null;
        if (turn.startEntryId === null) return;
        yield* emit({
          type: "provider_turn.updated",
          driver,
          threadId: turn.turnInput.threadId,
          providerTurn: { ...turn.providerTurn, nativeTurnRef: providerRef(turn.startEntryId) },
        });
      });

      const finalizeTurn = Effect.fnUntraced(function* (state: PiThreadState, readUsage = true) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        const completedAt = yield* DateTime.now;
        yield* completeOpenStreamItems(turn);
        yield* closeFinishingUp(turn);
        if (turn.activeCompaction !== null) {
          const status = turn.interrupted
            ? "cancelled"
            : turn.failure === null
              ? "completed"
              : "failed";
          yield* emitCompaction(turn, turn.activeCompaction, status);
          turn.activeCompaction = null;
        }
        if (turn.activeProviderRetry !== null) {
          if (turn.interrupted) {
            yield* emitProviderRetry(turn, turn.activeProviderRetry, "interrupted", completedAt);
            turn.activeProviderRetry = null;
          } else if (turn.failure === null) {
            yield* emitProviderRetry(turn, turn.activeProviderRetry, "completed", completedAt);
            turn.activeProviderRetry = null;
          }
        }
        yield* cancelPendingPrompts(completedAt);
        // Stop restarts the process, which also ends this turn's children;
        // otherwise they keep running as background work.
        if (turn.interrupted) {
          for (const child of Array.from(rlmChildren.values())) {
            if (child.turn === turn) yield* emitRlmChild(child.snapshot, null, "interrupted");
          }
        }
        const treeRefs =
          turn.stopTreeRefs !== undefined ? turn.stopTreeRefs : yield* captureTurnTreeRefs();
        const turnStartEntryId = turn.startEntryId ?? treeRefs?.turnStartEntryId ?? null;
        const tokenUsage = readUsage
          ? yield* readTokenUsage(turn.latestCompactionAfterTokens, completedAt)
          : undefined;
        const failure = turn.interrupted ? null : turn.failure;
        yield* emit({
          type: "provider_turn.updated",
          driver,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            ...(turnStartEntryId === null ? {} : { nativeTurnRef: providerRef(turnStartEntryId) }),
            status: turn.interrupted ? "interrupted" : failure !== null ? "failed" : "completed",
            completedAt,
            ...(tokenUsage === undefined ? {} : { tokenUsage }),
          },
        });
        yield* updateProviderThread(state, {
          status: "idle",
          ...(treeRefs?.leafId == null
            ? {}
            : { nativeConversationHeadRef: providerRef(treeRefs.leafId) }),
        });
        yield* updateProviderSession(
          failure !== null ? "error" : "ready",
          failure?.message ?? null,
        );
        if (failure !== null) {
          const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
          if (turn.activeProviderRetry !== null) {
            yield* emitProviderRetry(
              turn,
              { ...turn.activeProviderRetry, failure },
              "failed",
              completedAt,
            );
          } else {
            yield* emit({
              type: "turn_item.updated",
              driver,
              turnItem: {
                ...baseItemFields(turn, failureItemId, completedAt, completedAt),
                status: "failed",
                title: null,
                completedAt,
                type: "error",
                failure,
              },
            });
          }
          yield* emit({
            type: "turn.terminal",
            driver,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: itemOrdinal(turn, failureItemId),
            status: "failed",
            failure,
            ...(turn.activeProviderRetry === null
              ? {}
              : {
                  retry: turn.activeProviderRetry.retry,
                  retryStartedAt: turn.activeProviderRetry.startedAt,
                }),
            threadDisposition: "reusable",
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            status: turn.interrupted ? "interrupted" : "completed",
            failure: null,
            threadDisposition: "reusable",
          });
        }
      });

      // ── event pump ────────────────────────────────────────

      const scheduleSettleProbe = (
        turn: ActivePiTurn,
        settleAfterAgentActivity = false,
        attempt = 1,
        busyPolls = 0,
      ) => {
        const providerTurnId = turn.providerTurn.id;
        const settleProbeGeneration = turn.settleProbeGeneration;
        return request({ type: "get_state" }, 2_000).pipe(
          Effect.matchEffect({
            onSuccess: (data) =>
              Queue.offer(connection.events, {
                type: "t3.settle_probe",
                providerTurnId,
                settleAfterAgentActivity,
                settleProbeGeneration,
                attempt,
                busyPolls,
                data,
              }),
            // A failed probe still has to reach the pump. Dropping it would
            // leave a command-only turn active forever, because Pi never emits
            // agent events for one.
            onFailure: () =>
              Queue.offer(connection.events, {
                type: "t3.settle_probe",
                providerTurnId,
                settleAfterAgentActivity,
                settleProbeGeneration,
                attempt,
                probeFailed: true,
              }),
          }),
          Effect.ignore,
          Effect.forkIn(scope),
        );
      };

      /**
       * Ask the orchestrator for a run to attach this wake to. Deferred to the
       * first message so the notification can say what woke the agent.
       */
      const offerWakeContinuation = Effect.fnUntraced(function* (
        wake: PendingPiWake,
        state: PiThreadState,
      ) {
        if (wake.offered || options.continuationRequests === undefined) return;
        wake.offered = true;
        const generation = wake.generation;
        yield* Effect.logInfo("orchestration-v2.pi-wake-turn-detected", {
          driver,
          providerSessionId: input.providerSessionId,
          providerThreadId: state.providerThread.id,
        });
        yield* options.continuationRequests.offer({
          threadId: state.providerThread.appThreadId ?? input.threadId,
          providerThreadId: state.providerThread.id,
          driver,
          detail: null,
          notification: piWakeNotification(wake.events, name),
          // A user turn that adopted the wake first makes this request stale.
          dispatchIfCurrent: (dispatch) =>
            pendingWake?.generation === generation
              ? Effect.map(dispatch, Option.some)
              : Effect.succeed(Option.none()),
        });
      });

      const handleSessionEvent = Effect.fnUntraced(function* (event: PiRpcRecord) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (turn === null && pendingWake !== null && isPiWakeEvent(event)) {
          pendingWake.events.push(event);
          if (event["type"] === "message_start") yield* completeBackgroundJob(event["message"]);
          // Wait for the agent's own reply, so every message it was woken
          // with (kernel restore notices come first) is in the buffer.
          const agentReplied =
            event["type"] === "message_start" &&
            recordString(event["message"], "role") === "assistant";
          if (state !== null && (agentReplied || event["type"] === "agent_end")) {
            yield* offerWakeContinuation(pendingWake, state);
          }
          return;
        }
        switch (event["type"]) {
          case "agent_start": {
            if (turn === null && flavor.selfWakes === "continuation" && state !== null) {
              pendingWake = { events: [event], offered: false, generation: ++wakeGeneration };
              return;
            }
            if (turn === null) {
              unsolicitedActivityDetected = true;
              yield* updateProviderSession("error", unsolicitedActivityError);
              yield* connection.terminate;
              return;
            }
            turn.sawAgentActivity = true;
            turn.settleProbeGeneration += 1;
            yield* closeFinishingUp(turn);
            return;
          }
          case "turn_start": {
            // The run went on after the reply, e.g. to answer a steer.
            if (turn !== null) yield* closeFinishingUp(turn);
            return;
          }
          case "turn_end": {
            if (turn === null) return;
            const message = event["message"];
            const content = recordField(message, "content");
            const callsTools =
              Array.isArray(content) &&
              content.some((part) => recordString(part, "type") === "toolCall");
            const stopReason = recordString(message, "stopReason");
            if (
              recordString(message, "role") === "assistant" &&
              !callsTools &&
              stopReason !== "error" &&
              stopReason !== "aborted"
            ) {
              yield* openFinishingUp(turn);
            }
            return;
          }
          case "refine_complete":
          case "refine_failed": {
            yield* emitRefineOutcome(event);
            return;
          }
          case "message_start": {
            yield* completeBackgroundJob(event["message"]);
            if (turn !== null && recordString(event["message"], "role") === "assistant") {
              yield* closeFinishingUp(turn);
              turn.sawAgentActivity = true;
              if (!adoptMessageIdentity(turn, event["message"])) turn.messageOrdinal += 1;
              // Pi persists the prompt before it emits the reply, so the
              // first reply is the earliest point the start entry is readable.
              // Pi's recorded replay fixtures predate this read, so only the
              // Prime Agent flavor takes it.
              if (flavor.sessionTree === "fork_messages" && turn.startEntryId === undefined) {
                yield* recordTurnStartEntry(turn);
              }
            }
            return;
          }
          case "message_update": {
            if (turn === null) return;
            turn.sawAgentActivity = true;
            yield* reportLiveUsage(turn, event["usage"]);
            const delta = event["assistantMessageEvent"];
            const deltaType = recordString(delta, "type");
            const contentIndex = recordNumber(delta, "contentIndex") ?? 0;
            adoptMessageIdentity(turn, event["message"]);
            if (flavor.lossyStream) yield* adoptSnapshot(turn, event["message"], false);
            if (deltaType === "text_delta" || deltaType === "thinking_delta") {
              const item = yield* streamItemFor(
                turn,
                deltaType === "text_delta" ? "assistant_message" : "reasoning",
                contentIndex,
              );
              // A snapshot already holds this delta.
              if (!(flavor.lossyStream && snapshotBlock(event["message"], contentIndex))) {
                item.text += recordString(delta, "delta") ?? "";
              }
              yield* scheduleStreamFlush(turn, item);
              return;
            }
            if (deltaType === "text_end" || deltaType === "thinking_end") {
              const item = yield* streamItemFor(
                turn,
                deltaType === "text_end" ? "assistant_message" : "reasoning",
                contentIndex,
              );
              yield* completeStreamItem(
                turn,
                item,
                recordString(delta, "content") ?? recordString(delta, "thinking"),
              );
              return;
            }
            return;
          }
          case "message_end": {
            if (turn === null) return;
            const message = event["message"];
            if (recordString(message, "role") !== "assistant") return;
            adoptMessageIdentity(turn, message);
            if (flavor.lossyStream) yield* adoptSnapshot(turn, message, true);
            yield* completeOpenStreamItems(turn);
            if (recordString(message, "stopReason") === "error" && turn.failure === null) {
              turn.failure = makeProviderFailure({
                message: recordString(message, "errorMessage") ?? `${name} reported a model error.`,
                class: "provider_error",
              });
            }
            return;
          }
          case "tool_execution_start":
            if (turn !== null) {
              yield* closeFinishingUp(turn);
              turn.sawAgentActivity = true;
              yield* emitToolItem(turn, event, "start");
            }
            return;
          case "tool_execution_update":
            if (turn !== null) yield* emitToolItem(turn, event, "update");
            return;
          case "tool_execution_end":
            if (turn !== null) yield* emitToolItem(turn, event, "end");
            return;
          case "compaction_start": {
            if (turn === null) return;
            turn.settleProbeGeneration += 1;
            turn.sawCompaction = true;
            if (turn.activeCompaction !== null) {
              yield* emitCompaction(turn, turn.activeCompaction, "cancelled");
            }
            const startedAt = yield* DateTime.now;
            const compaction = {
              nativeItemId: compactionNativeItemId(turn),
              startedAt,
            } satisfies PiCompactionState;
            turn.activeCompaction = compaction;
            yield* emitCompaction(turn, compaction, "running");
            return;
          }
          case "compaction_end": {
            if (turn === null) return;
            const observedAt = yield* DateTime.now;
            const compaction = turn.activeCompaction ?? {
              nativeItemId: compactionNativeItemId(turn),
              startedAt: observedAt,
            };
            turn.activeCompaction = null;
            const result = event["result"];
            if (result === null || result === undefined) {
              if (event["aborted"] === true) {
                yield* emitCompaction(turn, compaction, "cancelled");
                if (turn.settleWhenIdle || !turn.sawAgentActivity) {
                  yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
                }
                return;
              }
              const errorMessage =
                recordString(event, "errorMessage") ?? `${name} context compaction failed.`;
              yield* emitCompaction(turn, compaction, "failed", {
                summary: errorMessage.slice(0, 1_000),
              });
              if (turn.settleWhenIdle || !turn.sawAgentActivity) {
                yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
              }
              return;
            }
            // An overflow can surface as a model error (`message_end` with
            // stopReason error) before Pi compacts and retries the turn. Clear
            // that failure only when Pi confirms that compaction will retry;
            // a successful non-retrying compaction must not erase an exhausted
            // provider retry.
            if (event["willRetry"] === true) turn.failure = null;
            turn.latestCompactionAfterTokens =
              nonNegativeInteger(result, "estimatedTokensAfter") ?? null;
            const summary = recordString(result, "summary");
            const beforeTokenCount = nonNegativeInteger(result, "tokensBefore");
            const afterTokenCount = nonNegativeInteger(result, "estimatedTokensAfter");
            yield* emitCompaction(turn, compaction, "completed", {
              ...(summary === undefined ? {} : { summary }),
              ...(beforeTokenCount === undefined ? {} : { beforeTokenCount }),
              ...(afterTokenCount === undefined ? {} : { afterTokenCount }),
            });
            if (turn.settleWhenIdle || !turn.sawAgentActivity) {
              yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
            }
            return;
          }
          case "auto_retry_start": {
            if (turn === null) return;
            const emittedAt = yield* DateTime.now;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            const maxAttempts = Math.max(
              attempt,
              Math.trunc(recordNumber(event, "maxAttempts") ?? attempt),
            );
            const retryDelayMs = Math.max(0, Math.trunc(recordNumber(event, "delayMs") ?? 0));
            const failure = makeProviderFailure({
              message: recordString(event, "errorMessage") ?? `${name} provider request failed.`,
              class: "provider_error",
              retryable: true,
            });
            const current = turn.activeProviderRetry;
            const providerRetry = {
              retry: { attempt, maxAttempts, retryDelayMs },
              failure,
              startedAt: current?.startedAt ?? emittedAt,
              itemOrdinal:
                current?.itemOrdinal ??
                itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
            } satisfies PiProviderRetryState;
            turn.activeProviderRetry = providerRetry;
            yield* emitProviderRetry(turn, providerRetry, "running", emittedAt);
            return;
          }
          case "auto_retry_end": {
            if (turn === null) return;
            const emittedAt = yield* DateTime.now;
            if (event["success"] === true) {
              // The retry recovered. Pi emits the erroring `message_end`
              // before retrying, so leaving that failure in place would make
              // `agent_settled` terminalize a successful turn as failed.
              if (turn.activeProviderRetry !== null) {
                const attempt = Math.max(
                  1,
                  Math.trunc(
                    recordNumber(event, "attempt") ?? turn.activeProviderRetry.retry.attempt,
                  ),
                );
                const recoveredRetry = {
                  ...turn.activeProviderRetry,
                  retry: { ...turn.activeProviderRetry.retry, attempt },
                };
                yield* emitProviderRetry(turn, recoveredRetry, "completed", emittedAt);
                turn.activeProviderRetry = null;
              }
              turn.failure = null;
              return;
            }
            const failure = makeProviderFailure({
              message: recordString(event, "finalError") ?? `${name} auto-retry failed.`,
              class: "provider_error",
              retryable: false,
            });
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            const current = turn.activeProviderRetry;
            const providerRetry = {
              retry: {
                attempt,
                maxAttempts: current?.retry.maxAttempts ?? attempt,
                retryDelayMs: current?.retry.retryDelayMs ?? null,
              },
              failure,
              startedAt: current?.startedAt ?? emittedAt,
              itemOrdinal:
                current?.itemOrdinal ??
                itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
            } satisfies PiProviderRetryState;
            turn.activeProviderRetry = providerRetry;
            turn.failure = failure;
            yield* emitProviderRetry(turn, providerRetry, "failed", emittedAt);
            return;
          }
          case "extension_ui_request":
            yield* handleExtensionUiRequest(event);
            return;
          case "extension_error": {
            yield* emitExtensionError(event);
            return;
          }
          case "agent_end": {
            // Pi follows agent_end with agent_settled once retries, compaction,
            // and queued continuations are done. Prime Agent never sends that,
            // so treat each agent_end as a candidate settle point and confirm
            // it with get_state.
            if (flavor.settleSignal !== "idle_probe" || turn === null) return;
            if (turn.interrupted) {
              if (state !== null) yield* finalizeTurn(state);
              return;
            }
            turn.settleWhenIdle = true;
            turn.settleProbeGeneration += 1;
            yield* scheduleSettleProbe(turn, true);
            return;
          }
          case "rlm_child_update": {
            yield* emitRlmChild(event["child"], turn);
            return;
          }
          case "observed_session_event": {
            const sessionId = recordString(event, "activeSessionId");
            const transcript =
              sessionId === undefined ? undefined : observedTranscripts.get(sessionId);
            const observed = event["event"];
            if (transcript !== undefined && Predicate.isObject(observed)) {
              yield* observeChildEvent(transcript, observed);
            }
            return;
          }
          case "observed_session_closed": {
            const sessionId = recordString(event, "activeSessionId");
            const transcript =
              sessionId === undefined ? undefined : observedTranscripts.get(sessionId);
            if (sessionId !== undefined && transcript !== undefined) {
              // The session is gone; observing it again would only fail.
              transcript.failedSessionIds.add(sessionId);
              yield* stopObservingChild(transcript, false);
            }
            return;
          }
          case "agent_settled": {
            if (turn?.interrupted === true) {
              if (state !== null) yield* finalizeTurn(state);
              return;
            }
            if (turn !== null) {
              turn.settleWhenIdle = true;
              turn.settleProbeGeneration += 1;
              yield* scheduleSettleProbe(turn, true);
            }
            return;
          }
          case "response": {
            // Correlated responses never reach the pump; an id-less response
            // is the deferred ack of a fire-and-forget prompt/steer/compact.
            const command = recordString(event, "command");
            if (command === "compact") {
              const pendingCompact = pendingCompactResponses.shift();
              const compactTurn =
                pendingCompact?.providerTurnId === turn?.providerTurn.id ? turn : null;
              if (compactTurn !== null) compactTurn.manualCompactInFlight = false;
              if (event["success"] === true) {
                if (
                  pendingCompact?.kind === "turn_start" &&
                  compactTurn !== null &&
                  compactTurn.promptMayBeCommandOnly &&
                  !compactTurn.sawAgentActivity
                ) {
                  yield* scheduleSettleProbe(compactTurn);
                }
                return;
              }
              if (event["success"] !== false) return;
              if (compactTurn === null) return;
              if (compactTurn.activeCompaction !== null) return;
              if (pendingCompact?.kind === "steer") {
                yield* Effect.logWarning(`${name} rejected a compact steer.`, {
                  errorLength: recordString(event, "error")?.length,
                });
                return;
              }
              if (!compactTurn.sawCompaction) {
                compactTurn.failure = makeProviderFailure({
                  message: recordString(event, "error") ?? `${name} compact failed.`,
                  class: "provider_error",
                });
                if (state !== null) yield* finalizeTurn(state);
                return;
              }
              if (!compactTurn.sawAgentActivity) {
                yield* scheduleSettleProbe(compactTurn);
              }
              return;
            }
            const pendingPrompt = command === "prompt" ? pendingPromptResponses.shift() : undefined;
            const responseTurn =
              pendingPrompt?.providerTurnId === turn?.providerTurn.id ? turn : null;
            if (event["success"] === true) {
              // Deferred success ack. Command-only prompts (pure extension
              // slash commands) never start an agent run and never emit
              // `agent_settled`, so probe for idleness. The probe result is
              // re-queued behind any events Pi emitted before answering
              // get_state, which keeps the check stream-ordered.
              if (
                pendingPrompt?.kind === "turn_start" &&
                responseTurn !== null &&
                responseTurn.promptMayBeCommandOnly &&
                !responseTurn.sawAgentActivity
              ) {
                yield* scheduleSettleProbe(responseTurn);
              }
              return;
            }
            if (event["success"] !== false) return;
            if (command === "steer" || pendingPrompt?.kind === "steer") {
              // A rejected steer only means that one message was refused. The
              // turn it was aimed at is still running on Pi, so terminalizing
              // here would report a failure while output keeps streaming.
              yield* Effect.logWarning(`${name} rejected a steer message.`, {
                errorLength: recordString(event, "error")?.length,
              });
              return;
            }
            const failedTurn =
              command === "prompt" && pendingPrompt?.kind === "turn_start"
                ? responseTurn
                : command === "parse"
                  ? turn
                  : null;
            if (failedTurn !== null) {
              failedTurn.failure = makeProviderFailure({
                message: recordString(event, "error") ?? `${name} rejected the prompt.`,
                class: "provider_error",
              });
              if (state !== null) yield* finalizeTurn(state);
            }
            return;
          }
          case "t3.flush_extension_errors": {
            // Startup extension failures are informational and do not block
            // Pi, so attach them to the next real turn instead of creating a
            // standalone failed run.
            for (const extensionError of outOfTurnExtensionErrors.splice(0)) {
              yield* emitExtensionError(extensionError);
            }
            return;
          }
          case "t3.settle_probe": {
            // New work increments the generation before the pump can consume
            // a stale idle snapshot, so only a current snapshot may settle.
            const data = event["data"];
            const probeFailed = event["probeFailed"] === true;
            const settleAfterAgentActivity = event["settleAfterAgentActivity"] === true;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            if (
              turn === null ||
              turn.providerTurn.id !== event["providerTurnId"] ||
              turn.settleProbeGeneration !== event["settleProbeGeneration"] ||
              (!settleAfterAgentActivity && turn.sawAgentActivity) ||
              turn.activeCompaction !== null
            ) {
              return;
            }
            if (probeFailed) {
              if (!settleAfterAgentActivity) {
                if (state !== null) yield* finalizeTurn(state);
                return;
              }
              if (attempt < SETTLE_PROBE_MAX_ATTEMPTS) {
                yield* Effect.sleep(SETTLE_PROBE_RETRY_DELAY).pipe(
                  Effect.andThen(scheduleSettleProbe(turn, true, attempt + 1)),
                  Effect.forkIn(scope),
                );
                return;
              }
              stopRequested = true;
              yield* connection.terminate;
              return;
            }
            if (piStateIsIdle(data)) {
              turn.settleWhenIdle = false;
              if (state !== null) yield* finalizeTurn(state);
              return;
            }
            // Pi follows busy work with events that re-probe. Without
            // agent_settled, the end of a retry wait or a queued action has no
            // event of its own, so keep reading state until it goes idle.
            // New work bumps the generation, which retires this chain.
            if (flavor.settleSignal === "idle_probe") {
              const busyPolls = Math.max(0, Math.trunc(recordNumber(event, "busyPolls") ?? 0));
              const delayMs = Math.min(
                BUSY_PROBE_INITIAL_DELAY_MS * 2 ** busyPolls,
                BUSY_PROBE_MAX_DELAY_MS,
              );
              yield* Effect.sleep(Duration.millis(delayMs)).pipe(
                Effect.andThen(
                  scheduleSettleProbe(turn, settleAfterAgentActivity, 1, busyPolls + 1),
                ),
                Effect.forkIn(scope),
              );
            }
            return;
          }
          default:
            return;
        }
      });

      yield* Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(connection.events);
          yield* sessionEventPermit.withPermits(1)(handleSessionEvent(event));
        }
      }).pipe(
        Effect.catchCause((cause) =>
          sessionEventPermit.withPermits(1)(
            Effect.gen(function* () {
              // Transport death finalizes any live turn. Stop-with-restart
              // closes the provider stream cleanly; only an unexpected death
              // is surfaced as an event-stream failure.
              pendingWake = null;
              const state = threadState;
              const interrupted = state?.activeTurn?.interrupted === true;
              // The kernel and its children died with the process.
              for (const child of Array.from(rlmChildren.values())) {
                if (!child.terminal) yield* emitRlmChild(child.snapshot, null, "interrupted");
              }
              if (backgroundJobs.size > 0) {
                backgroundJobs.clear();
                yield* publishBackgroundJobs();
              }
              if (state?.activeTurn != null) {
                state.activeTurn.failure = interrupted
                  ? null
                  : makeProviderFailure({
                      cause,
                      message: `${name} process exited unexpectedly.`,
                      class: "transport_error",
                    });
                yield* finalizeTurn(state, false);
              }
              if (unsolicitedActivityDetected) {
                yield* updateProviderSession("error", unsolicitedActivityError);
                yield* Queue.end(events);
              } else if (stopRequested) {
                yield* updateProviderSession("stopped", null);
                yield* Queue.end(events);
              } else {
                yield* updateProviderSession(
                  "error",
                  interrupted
                    ? `${name} process was stopped.`
                    : `${name} process exited unexpectedly.`,
                );
                yield* Queue.fail(
                  events,
                  new ProviderAdapter.ProviderAdapterEventStreamError({
                    driver,
                    providerSessionId: input.providerSessionId,
                    cause,
                  }),
                );
              }
            }),
          ),
        ),
        Effect.forkIn(scope),
      );

      // Discovery can invoke extension code and therefore raise a blocking
      // UI request. Start it only after the event pump exists, and never hold
      // session opening on it; startup requests are persisted at session
      // scope and can be answered before a turn begins.
      yield* discoverSkillNames.pipe(
        Effect.tap((discovered) => Effect.sync(() => (skillNames = discovered))),
        Effect.ignore,
        Effect.forkIn(scope),
      );

      // ── session runtime ───────────────────────────────────

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        publish = true,
      ) {
        if (threadState !== null && threadState.activeTurn !== null) {
          return yield* protocolError("Cannot register a Pi thread while a turn is active");
        }
        const existing = threadInput.existingProviderThread;
        const resumeId = existing?.nativeThreadRef?.nativeId;
        const needsNewSession = resumeId == null && registrationAttempted;
        registrationAttempted = true;
        if (resumeId != null || needsNewSession) {
          lastNativeThreadId = resumeId ?? lastNativeThreadId;
          // Even a failed lifecycle operation can change Pi's native session.
          // Never leave the old app binding or model defaults usable afterward.
          threadState = null;
          appliedModel = null;
          appliedThinking = null;
          appliedSessionName = null;
          baselineModel = null;
          baselineThinking = null;
          contextWindow = null;
          const result = yield* lifecycleRequest(
            resumeId != null
              ? { type: "switch_session", sessionPath: resumeId }
              : { type: "new_session" },
          );
          if (recordField(result, "cancelled") === true) {
            return yield* protocolError("A Pi extension cancelled the session switch");
          }
        }
        const stateData = yield* request({ type: "get_state" });
        if (!modelsDiscovered) {
          const modelsData = yield* request({ type: "get_available_models" }).pipe(
            Effect.orElseSucceed(() => undefined),
          );
          const models = recordField(modelsData, "models");
          if (Array.isArray(models)) {
            for (const model of models) rememberModelContextWindow(model);
            modelsDiscovered = true;
          }
        }
        contextWindow = rememberModelContextWindow(recordField(stateData, "model"));
        // Each baseline is captured independently, and only while nothing has
        // been applied yet, so a `get_state` that arrives after our own
        // selection cannot record that selection as Pi's default.
        if (baselineModel === null && appliedModel === null) {
          const stateModel = recordField(stateData, "model");
          const provider = recordString(stateModel, "provider");
          const modelId = recordString(stateModel, "id");
          if (provider !== undefined && modelId !== undefined) {
            baselineModel = { provider, modelId };
          }
        }
        if (baselineThinking === null && appliedThinking === null) {
          baselineThinking = recordString(stateData, "thinkingLevel") ?? null;
        }
        // switch_session accepts a file path, not Pi's display session UUID.
        const nativeId = recordString(stateData, "sessionFile");
        if (nativeId === undefined) {
          return yield* protocolError("get_state returned no persisted sessionFile", stateData);
        }
        if (needsNewSession && nativeId === lastNativeThreadId) {
          return yield* protocolError(`${name} did not create a distinct session file`);
        }
        lastNativeThreadId = nativeId;
        const createdAt = yield* DateTime.now;
        const providerThread: OrchestrationV2ProviderThread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                nativeThreadRef: providerRef(nativeId),
                ...(needsNewSession ? { nativeConversationHeadRef: null, contextUsage: null } : {}),
                status: "idle",
                updatedAt: createdAt,
              }
            : {
                id: idAllocator.derive.providerThread({
                  driver,
                  nativeThreadId: nativeId,
                }),
                driver,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: providerRef(nativeId),
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                createdAt,
                updatedAt: createdAt,
              };
        threadState = { providerThread, activeTurn: null };
        // Baseline the session tree so the first turn's user entry can be
        // located without a full scan.
        yield* baselineSessionTree();
        if (publish)
          yield* emit({
            type: "provider_thread.updated",
            driver,
            providerThread,
          });
        return providerThread;
      });

      const applySelection = Effect.fnUntraced(function* (modelSelection: ModelSelection) {
        const thinking = getModelSelectionStringOptionValue(modelSelection, "thinking");
        if (modelSelection.model === PI_INHERIT_MODEL_SLUG) {
          // Returning to "Pi default" after an explicit pick has to replay the
          // captured baseline, otherwise Pi stays on the last model applied.
          if (appliedModel !== null && baselineModel !== null) {
            const restoredModel = yield* request({ type: "set_model", ...baselineModel });
            contextWindow = rememberModelContextWindow(restoredModel);
            appliedModel = null;
            const updatedAt = yield* DateTime.now;
            sessionEntity = { ...sessionEntity, model: PI_INHERIT_MODEL_SLUG, updatedAt };
            yield* emit({
              type: "provider_session.updated",
              driver,
              providerSession: sessionEntity,
            });
          }
          // The "Pi default" model advertises no thinking choices of its own,
          // so an unqualified return also restores Pi's configured level
          // instead of silently keeping the effort a previous pick applied.
          if (
            thinking === undefined &&
            appliedThinking !== null &&
            baselineThinking !== null &&
            appliedThinking !== baselineThinking
          ) {
            yield* request({ type: "set_thinking_level", level: baselineThinking });
            appliedThinking = null;
          }
        } else if (modelSelection.model !== appliedModel) {
          const parsed = parsePiModelSlug(modelSelection.model);
          if (parsed === null) {
            return yield* protocolError(
              `${name} model '${modelSelection.model}' must use provider/model format`,
            );
          }
          const selectedModel = yield* request({
            type: "set_model",
            provider: parsed.provider,
            modelId: parsed.modelId,
          });
          contextWindow = rememberModelContextWindow(selectedModel);
          appliedModel = modelSelection.model;
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, model: modelSelection.model, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver,
            providerSession: sessionEntity,
          });
        }
        if (
          thinking !== undefined &&
          thinking !== appliedThinking &&
          PI_THINKING_LEVELS.has(thinking)
        ) {
          yield* request({ type: "set_thinking_level", level: thinking });
          appliedThinking = thinking;
        }
      });

      const resolvePromptPayload = Effect.fnUntraced(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        // Provider discovery and the live session are separate Pi processes.
        // Retry a failed session-local lookup once at first use so a transient
        // startup failure cannot leave a visible $ skill inert for this session.
        if (skillNames === null && text.includes("$")) {
          skillNames = yield* discoverSkillNames.pipe(
            Effect.orElseSucceed(() => new Set<string>()),
          );
        }
        const expandedText = skillNames === null ? text : expandPiSkillReference(text, skillNames);
        const images: Array<{ type: "image"; data: string; mimeType: string }> = [];
        const extraLines: Array<string> = [];
        for (const attachment of attachments) {
          const path = resolveAttachmentPath({
            attachmentsDir: options.serverConfig.attachmentsDir,
            attachment,
          });
          if (path === null) continue;
          if (attachment.mimeType.startsWith("image/")) {
            const bytes = yield* options.fileSystem.readFile(path);
            images.push({
              type: "image",
              data: Buffer.from(bytes).toString("base64"),
              mimeType: attachment.mimeType,
            });
          } else {
            extraLines.push(`[Attachment saved at ${path}]`);
          }
        }
        const message =
          extraLines.length === 0 ? expandedText : `${expandedText}\n\n${extraLines.join("\n")}`;
        return { message, images };
      });

      /**
       * Rolls the conversation back to just before `entryId` inside the same
       * session file. RPC has no tree navigation, so T3's extension command
       * calls `ctx.navigateTree` and reports back through a marked notify.
       */
      const navigateTreeInPlace = Effect.fnUntraced(function* (entryId: string) {
        const requestId = `t3-nav-${++treeNavigationCounter}`;
        const outcome = yield* Deferred.make<unknown>();
        pendingTreeNavigations.set(requestId, outcome);
        const result = yield* Effect.gen(function* () {
          yield* request(
            { type: "prompt", message: `/${T3_NAVIGATE_TREE_COMMAND} ${requestId} ${entryId}` },
            PI_SESSION_TIMEOUT_MS,
          );
          return yield* Deferred.await(outcome).pipe(
            Effect.timeoutOrElse({
              duration: Duration.millis(PI_SESSION_TIMEOUT_MS),
              orElse: () =>
                Effect.fail(protocolError(`${name} did not report the in-place rollback`)),
            }),
          );
        }).pipe(Effect.ensuring(Effect.sync(() => pendingTreeNavigations.delete(requestId))));
        const status = recordString(result, "outcome");
        if (status === "ok") return;
        return yield* protocolError(
          status === "cancelled"
            ? `A ${name} extension cancelled the rollback`
            : `${name} rollback failed: ${recordString(result, "error") ?? "unknown error"}`,
        );
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        getModelContextWindow: (selection) => {
          if (selection.instanceId !== options.instanceId) return undefined;
          const slug =
            selection.model === PI_INHERIT_MODEL_SLUG
              ? baselineModel === null
                ? undefined
                : `${baselineModel.provider}/${baselineModel.modelId}`
              : selection.model;
          return slug === undefined ? undefined : modelContextWindows.get(slug);
        },
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        // The run executor routes a bare `/compact` here instead of startTurn.
        // Pi's start path already turns that text into the RPC compact call.
        compactThread: (turnInput) =>
          runtime.startTurn({
            ...turnInput,
            message: { ...turnInput.message, text: "/compact" },
          }),
        startTurn: (turnInput) =>
          Effect.gen(function* () {
            const state = threadState;
            if (state === null) {
              return yield* protocolError(`${name} session has no registered thread`);
            }
            if (state.activeTurn !== null) {
              return yield* protocolError(
                `${name} provider thread ${turnInput.providerThread.id} already has an active turn`,
              );
            }
            if (
              state.providerThread.nativeThreadRef?.nativeId !==
              turnInput.providerThread.nativeThreadRef?.nativeId
            ) {
              return yield* protocolError(`${name} turn requested for a different native session`);
            }
            // The orchestrator adopts a fork under its already-allocated row.
            // Future session updates must retain that authoritative identity.
            state.providerThread = turnInput.providerThread;
            yield* applySelection(turnInput.modelSelection);
            // Mirror the thread title into pi's session name so the session
            // stays identifiable in pi's own /resume listing. Best-effort:
            // naming must never block a turn.
            if (turnInput.appThread.title !== appliedSessionName) {
              yield* request({
                type: "set_session_name",
                name: turnInput.appThread.title,
              }).pipe(
                Effect.tap(() =>
                  Effect.sync(() => (appliedSessionName = turnInput.appThread.title)),
                ),
                Effect.ignore,
              );
            }
            // Resolved before the turn is installed: a failure here (an
            // unreadable attachment) must not leave `activeTurn` set, which
            // would reject every later turn as already active.
            // Orchestration instructions reach pi through the T3 MCP
            // extension's before_agent_start system-prompt hook, never by
            // wrapping the user text: a wrapped first message would no
            // longer start with "/" and slash commands would stop expanding.
            // A continuation run for a self-wake carries placeholder text; the
            // turn's content is the buffered wake, so nothing is prompted.
            const isWakeContinuation = turnInput.message.creationSource === "provider";
            const compactCommand = isWakeContinuation
              ? null
              : parsePiCompactCommand(turnInput.message.text);
            const payload =
              compactCommand === null && !isWakeContinuation
                ? yield* resolvePromptPayload(turnInput.message.text, turnInput.message.attachments)
                : null;
            const startedAt = yield* DateTime.now;
            const syntheticNativeTurnId = `${state.providerThread.id}:attempt:${turnInput.attemptId}`;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({
                driver,
                nativeTurnId: syntheticNativeTurnId,
              }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const activeTurn: ActivePiTurn = {
              turnInput,
              providerTurn,
              startedAt,
              itemOrdinals: new Map(),
              nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              messageOrdinal: 0,
              streamItems: new Map(),
              toolArgs: new Map(),
              toolStartedAt: new Map(),
              interrupted: false,
              sawAgentActivity: false,
              promptMayBeCommandOnly:
                isWakeContinuation ||
                compactCommand !== null ||
                (payload?.message.trimStart().startsWith("/") ?? false),
              latestCompactionAfterTokens: null,
              lastLiveUsedTokens: null,
              settleProbeGeneration: 0,
              settleWhenIdle: false,
              sawCompaction: false,
              manualCompactInFlight: compactCommand !== null,
              activeCompaction: null,
              activeProviderRetry: null,
              finishingUp: null,
              noticeCount: 0,
              failure: null,
            };
            // Only the install/send/start-event boundary excludes the event
            // pump. Earlier correlated requests must leave the pump free so
            // project trust, login, and session-switch dialogs can be shown
            // and answered instead of deadlocking the caller.
            yield* Effect.gen(function* () {
              state.activeTurn = activeTurn;
              // Read under the permit: the pump cannot buffer more wake events
              // between taking the buffer and installing the turn.
              const adoptedWake = pendingWake;
              pendingWake = null;
              if (compactCommand !== null) {
                yield* connection.send(compactRpcRecord(compactCommand));
                pendingCompactResponses.push({
                  providerTurnId: providerTurn.id,
                  kind: "turn_start",
                });
              } else if (payload !== null) {
                yield* connection.send({
                  type: "prompt",
                  message: payload.message,
                  // The agent is still busy with a wake; queue behind it.
                  ...(adoptedWake === null ? {} : { streamingBehavior: "followUp" }),
                  ...(payload.images.length === 0 ? {} : { images: payload.images }),
                });
                pendingPromptResponses.push({
                  providerTurnId: providerTurn.id,
                  kind: "turn_start",
                });
              }
              yield* emit({
                type: "provider_turn.updated",
                driver,
                threadId: turnInput.threadId,
                providerTurn,
              });
              yield* updateProviderThread(state, {
                status: "active",
                firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
                lastRunOrdinal: turnInput.runOrdinal,
              });
              yield* updateProviderSession("running", null);
              if (adoptedWake !== null) {
                for (const wakeEvent of adoptedWake.events) yield* handleSessionEvent(wakeEvent);
              } else if (isWakeContinuation) {
                // The wake was already adopted by a user turn or ended with the
                // process. Settle this run as soon as the agent is idle.
                yield* scheduleSettleProbe(activeTurn);
              }
              if (outOfTurnExtensionErrors.length > 0) {
                yield* Queue.offer(connection.events, { type: "t3.flush_extension_errors" });
              }
            }).pipe(
              sessionEventPermit.withPermits(1),
              Effect.tapError(() =>
                Effect.sync(() => {
                  if (state.activeTurn === activeTurn) state.activeTurn = null;
                }),
              ),
            );
            // Pi acks `prompt` only after slash-command expansion completes,
            // and extension commands may block on user dialogs indefinitely.
            // Rejections therefore return later as id-less response records
            // handled by the event pump.
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: (steerInput: ProviderAdapter.ProviderAdapterV2SteerInput) =>
          Effect.gen(function* () {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null || turn.providerTurn.id !== steerInput.providerTurnId) {
              return yield* protocolError(
                `${name} turn ${steerInput.providerTurnId} is not active`,
              );
            }
            const compactCommand = parsePiCompactCommand(steerInput.message.text);
            const payload =
              compactCommand === null
                ? yield* resolvePromptPayload(
                    steerInput.message.text,
                    steerInput.message.attachments,
                  )
                : null;
            // Prompt with streamingBehavior steer is atomic on Pi's side: it
            // queues during an active run and starts a new run if settlement
            // won the race. A direct `steer` sent after Pi became idle would
            // remain queued forever. Send fire-and-forget under the session
            // permit so a slash-command dialog cannot block the turn, and so
            // settlement cannot overtake the active-turn check.
            // /compact is not a prompt: Pi's compact RPC aborts the agent first.
            yield* sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                if (threadState?.activeTurn !== turn) {
                  return yield* protocolError(
                    `${name} turn ${steerInput.providerTurnId} is not active`,
                  );
                }
                if (compactCommand !== null) {
                  turn.manualCompactInFlight = true;
                  yield* connection.send(compactRpcRecord(compactCommand));
                  pendingCompactResponses.push({
                    providerTurnId: turn.providerTurn.id,
                    kind: "steer",
                  });
                } else if (payload !== null) {
                  yield* connection.send({
                    type: "prompt",
                    message: payload.message,
                    streamingBehavior: "steer",
                    ...(payload.images.length === 0 ? {} : { images: payload.images }),
                  });
                  pendingPromptResponses.push({
                    providerTurnId: turn.providerTurn.id,
                    kind: "steer",
                  });
                }
                turn.settleProbeGeneration += 1;
              }),
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver,
                  providerThreadId: steerInput.providerThread.id,
                  providerTurnId: steerInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        interruptTurn: (interruptInput) =>
          Effect.gen(function* () {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null && hasPendingBackgroundWork()) {
              // Stop from the background-work banner after the turn settled.
              // Neither RPC nor extensions can cancel a subagent or a kernel
              // job, so end the session; it resumes on the next turn.
              stopRequested = true;
              yield* connection.terminate;
              return;
            }
            // Stop on a settled turn: Pi runs nothing between prompts, so
            // nothing of that turn is left to stop.
            if (turn === null && interruptInput.requestRuntimeRestart === true) return;
            if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
              return yield* protocolError(
                `${name} turn ${interruptInput.providerTurnId} is not active`,
              );
            }
            turn.interrupted = true;
            if (
              interruptInput.requestRuntimeRestart === true ||
              turn.settleWhenIdle ||
              turn.activeCompaction !== null ||
              turn.manualCompactInFlight ||
              // RPC cannot cancel a subagent, but ending the session stops it.
              hasLiveRlmChildren()
            ) {
              // Pi's generic abort does not cancel manual compaction. Terminate
              // so Stop covers user /compact as well as detached recovery compact.
              stopRequested = true;
              if (interruptInput.requestRuntimeRestart === true && !turn.settleWhenIdle) {
                yield* request({ type: "abort" }, 2_000).pipe(Effect.ignore);
              }
              // Terminating fails every later request, so read the stopped
              // turn's session-tree refs first: rolling back past this turn
              // forks at its user entry. Holding the event permit also lets a
              // finalize that is already reading them finish before the kill.
              yield* sessionEventPermit.withPermits(1)(
                Effect.gen(function* () {
                  if (threadState?.activeTurn === turn && turn.stopTreeRefs === undefined) {
                    turn.stopTreeRefs = yield* captureTurnTreeRefs(2_000);
                  }
                  yield* connection.terminate;
                }),
              );
              return;
            }
            yield* request({ type: "abort" }).pipe(
              Effect.tapError(() => Effect.sync(() => (turn.interrupted = false))),
            );
            // An abort during a retry wait or queued action ends no run, so
            // no agent_end follows. Idle-probe flavors settle from state.
            if (flavor.settleSignal === "idle_probe") {
              yield* sessionEventPermit.withPermits(1)(
                Effect.suspend(() => {
                  if (threadState?.activeTurn !== turn) return Effect.void;
                  turn.settleWhenIdle = true;
                  turn.settleProbeGeneration += 1;
                  return scheduleSettleProbe(turn, true).pipe(Effect.asVoid);
                }),
              );
            }
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterInterruptError({
                  driver,
                  providerThreadId: interruptInput.providerThread.id,
                  providerTurnId: interruptInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        respondToRuntimeRequest: (requestInput) =>
          Effect.gen(function* () {
            const pending = pendingPrompts.get(String(requestInput.requestId));
            if (pending === undefined) {
              return yield* protocolError(
                `No pending Pi extension request ${requestInput.requestId}`,
              );
            }
            const response = piUiResponse(pending, requestInput.decision, requestInput.answers);
            yield* connection.send({
              type: "extension_ui_response",
              id: pending.nativeRequestId,
              ...response,
            });
            // Dropped only once Pi has the answer, so a failed send leaves the
            // request retryable and still cancellable during teardown.
            pendingPrompts.delete(String(requestInput.requestId));
            if (pending.method === "confirm" && requestInput.decision === "acceptForSession") {
              sessionApprovals.add(pending.approvalKey);
            }
            const resolvedAt = yield* DateTime.now;
            pending.runtimeRequest = {
              ...pending.runtimeRequest,
              status: "resolved",
              resolvedAt,
            };
            yield* emit({
              type: "runtime_request.updated",
              driver,
              threadId: pending.node.threadId,
              runtimeRequest: pending.runtimeRequest,
            });
            yield* emit({
              type: "node.updated",
              driver,
              node: { ...pending.node, status: "completed", completedAt: resolvedAt },
            });
            yield* emit({
              type: "turn_item.updated",
              driver,
              turnItem: {
                ...pending.turnItem,
                status: "completed",
                completedAt: resolvedAt,
                updatedAt: resolvedAt,
              },
            });
          }).pipe(
            sessionEventPermit.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                  driver,
                  requestId: requestInput.requestId,
                  cause,
                }),
            ),
          ),
        hasPendingBackgroundWork: Effect.sync(hasPendingBackgroundWork),
        hasPendingBackgroundWorkForThread: (providerThread) =>
          Effect.sync(
            () =>
              threadState?.providerThread.id === providerThread.id && hasPendingBackgroundWork(),
          ),
        readThreadSnapshot: (snapshotInput) =>
          Effect.gen(function* () {
            const state = threadState;
            const boundNativeId = state?.providerThread.nativeThreadRef?.nativeId;
            const wantedNativeId = snapshotInput.providerThread.nativeThreadRef?.nativeId;
            if (state === null || wantedNativeId == null || boundNativeId !== wantedNativeId) {
              return yield* protocolError(
                `${name} snapshot requested for a thread this session does not host`,
              );
            }
            // get_messages is Pi's active-branch view. get_entries returns the
            // whole session tree, including abandoned branches after /tree or
            // fork, which would leak discarded conversation into handoffs.
            const messagesData = yield* request({ type: "get_messages" });
            const activeMessages = recordField(messagesData, "messages");
            const threadId = state.providerThread.appThreadId ?? input.threadId;
            const messages = (Array.isArray(activeMessages) ? activeMessages : []).flatMap(
              (message, index) => {
                const role = recordString(message, "role");
                if (role !== "user" && role !== "assistant") return [];
                const text = contentText(recordField(message, "content"));
                if (text.length === 0) return [];
                const timestamp = recordNumber(message, "timestamp");
                const at = Option.getOrElse(
                  DateTime.make(timestamp ?? Number.NaN),
                  () => state.providerThread.createdAt,
                );
                return [
                  {
                    id: idAllocator.derive.messageFromProviderItem({
                      driver,
                      // RPC messages do not expose session-tree entry ids. The
                      // active-branch index is stable for the lifetime of this
                      // snapshot and keeps abandoned branch ids out of it.
                      nativeItemId: `${wantedNativeId}:snapshot-message:${index}`,
                    }),
                    threadId,
                    runId: null,
                    nodeId: null,
                    role: role as "user" | "assistant",
                    text,
                    attachments: [],
                    streaming: false,
                    createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                    creationSource: "provider" as const,
                    createdAt: at,
                    updatedAt: at,
                  },
                ];
              },
            );
            return {
              providerThread: state.providerThread,
              providerTurns: [],
              messages,
              runtimeRequests: [],
            };
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver,
                  providerThreadId: snapshotInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        rollbackThread: (rollbackInput) =>
          Effect.gen(function* () {
            const state = threadState;
            if (state === null) {
              return yield* protocolError(`${name} session has no registered thread`);
            }
            if (state.providerThread.id !== rollbackInput.providerThread.id) {
              return yield* protocolError(
                `${name} rollback requested for a thread this session does not host`,
              );
            }
            if (state.activeTurn !== null) {
              return yield* protocolError("Cannot roll back while a Pi turn is active");
            }
            // `fork(entryId)` re-roots the active branch before that user
            // message, so the rollback boundary is the first user entry of
            // the earliest turn being discarded.
            const forkEntryId = piRollbackForkEntry(rollbackInput);
            if (forkEntryId === null) {
              // Nothing after the target: the conversation is already there.
              return piThreadSnapshot(state.providerThread);
            }
            if (forkEntryId === undefined) {
              return yield* protocolError(
                `${name} rollback target has no captured session-tree entry`,
              );
            }
            if (flavor.rollback === "tree") {
              const available =
                navigateTreeAvailable ??
                (yield* request({ type: "get_commands" }).pipe(
                  Effect.map(hasPiNavigateTreeCommand),
                  Effect.orElseSucceed(() => false),
                ));
              navigateTreeAvailable = available;
              if (available) {
                yield* navigateTreeInPlace(forkEntryId);
                // Same session file and model; only the branch head moved.
                yield* baselineSessionTree();
                yield* updateProviderThread(state, { nativeConversationHeadRef: null });
                return piThreadSnapshot(state.providerThread);
              }
            }
            const forkData = yield* lifecycleRequest({ type: "fork", entryId: forkEntryId });
            if (recordField(forkData, "cancelled") === true) {
              return yield* protocolError("A Pi extension cancelled the session fork");
            }
            // Pi fork replaces the session file, including for rollback. Persist
            // its new identity before any later request can fail or restart.
            const forkState = yield* request({ type: "get_state" }).pipe(
              Effect.tapError(() =>
                Effect.sync(() => {
                  threadState = null;
                }),
              ),
            );
            const forkSessionFile = recordString(forkState, "sessionFile");
            if (forkSessionFile === undefined) {
              threadState = null;
              return yield* protocolError(`${name} fork did not return a persisted session file`);
            }
            lastNativeThreadId = forkSessionFile;
            appliedModel = null;
            appliedThinking = null;
            appliedSessionName = null;
            yield* updateProviderThread(state, { nativeThreadRef: providerRef(forkSessionFile) });
            // The fork re-baselined the tree, so the cursor is trustworthy
            // again unless this listing itself failed.
            const leafId = yield* baselineSessionTree();
            yield* updateProviderThread(state, {
              nativeConversationHeadRef: leafId === null ? null : providerRef(leafId),
            });
            return piThreadSnapshot(state.providerThread);
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRollbackThreadError({
                  driver,
                  providerThreadId: rollbackInput.providerThread.id,
                  checkpointId: rollbackInput.target.checkpointId,
                  cause,
                }),
            ),
          ),
        forkThread: (forkInput) =>
          Effect.gen(function* () {
            if (threadState?.activeTurn != null) {
              return yield* protocolError("Cannot fork while a Pi turn is active");
            }
            const source = forkInput.sourceProviderThread;
            const sourceFile = source.nativeThreadRef?.nativeId;
            if (sourceFile == null)
              return yield* protocolError(`${name} fork source has no session file`);
            const sourceTurns = (forkInput.sourceProviderTurns ?? []).filter(
              (turn) => turn.providerThreadId === source.id,
            );
            const target = sourceTurns.find((turn) => turn.id === forkInput.providerTurnId);
            if (forkInput.providerTurnId !== undefined && target === undefined) {
              return yield* protocolError(`${name} fork target turn is missing`);
            }
            const beforeEntry =
              target === undefined
                ? null
                : piRollbackForkEntry({
                    target: { type: "provider_turn", providerTurn: target },
                    providerThreadTurns: sourceTurns,
                  });
            if (beforeEntry === undefined) {
              return yield* protocolError(
                `${name} fork boundary has no captured session-tree entry`,
              );
            }
            // CLI --fork uses Pi's own session format and sets the destination
            // cwd. RPC switch_session/clone alone would retain the source cwd.
            // This short-lived process never prompts or runs user extensions.
            const forkLaunch = buildPiRpcLaunch({
              launchArgs: resolvedLaunchArgs.args,
              environment: options.environment,
              mcpSession: undefined,
              extensionPath: undefined,
              disableExtensions: true,
              disableTools: true,
            });
            const nativeId = yield* Effect.scoped(
              Effect.gen(function* () {
                const forkConnection = yield* makePiRpcConnection({
                  command: binary,
                  args: [...forkLaunch.args, "--fork", sourceFile],
                  cwd,
                  env: forkLaunch.env,
                });
                yield* Stream.fromQueue(forkConnection.events).pipe(
                  Stream.runDrain,
                  Effect.ignore,
                  Effect.forkScoped,
                );
                if (beforeEntry !== null) {
                  const result = yield* forkConnection.request({
                    type: "fork",
                    entryId: beforeEntry,
                  });
                  if (recordField(result, "cancelled") === true) {
                    return yield* protocolError("A Pi extension cancelled the session fork");
                  }
                }
                const state = yield* forkConnection.request({ type: "get_state" });
                const file = recordString(state, "sessionFile");
                if (file === undefined || file === sourceFile) {
                  return yield* protocolError(
                    `${name} fork did not create a distinct session file`,
                  );
                }
                return file;
              }),
            ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, options.spawner));
            const now = yield* DateTime.now;
            return yield* registerThread(
              {
                threadId: forkInput.targetThreadId,
                modelSelection: forkInput.modelSelection ?? input.modelSelection,
                runtimePolicy: forkInput.runtimePolicy ?? input.runtimePolicy,
                existingProviderThread: {
                  ...source,
                  id: idAllocator.derive.providerThread({
                    driver,
                    nativeThreadId: nativeId,
                  }),
                  appThreadId: forkInput.targetThreadId,
                  providerSessionId: input.providerSessionId,
                  providerInstanceId: options.instanceId,
                  ownerNodeId: forkInput.ownerNodeId ?? null,
                  nativeThreadRef: providerRef(nativeId),
                  nativeConversationHeadRef: null,
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  pendingBackgroundTasks: [],
                  forkedFrom: {
                    providerThreadId: source.id,
                    ...(forkInput.providerTurnId === undefined
                      ? {}
                      : { providerTurnId: forkInput.providerTurnId }),
                  },
                  createdAt: now,
                  updatedAt: now,
                },
              },
              false,
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterForkThreadError({
                  driver,
                  providerThreadId: forkInput.sourceProviderThread.id,
                  cause,
                }),
            ),
          ),
      };
      return runtime;
    }),
  });
}

/**
 * Resolve the pi session-tree entry `fork` should re-root at for a rollback.
 * Returns `null` when no turns follow the target (nothing to discard) and
 * `undefined` when the boundary turn has no captured entry ref (only
 * turn-boundary refs recorded by `captureTurnTreeRefs` are strong).
 */
function piRollbackForkEntry(input: {
  readonly target:
    | { readonly type: "thread_start" }
    | { readonly type: "provider_turn"; readonly providerTurn: OrchestrationV2ProviderTurn };
  readonly providerThreadTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
}): string | null | undefined {
  const boundaryOrdinal =
    input.target.type === "thread_start" ? 0 : input.target.providerTurn.ordinal;
  const discarded = input.providerThreadTurns
    .filter((turn) => turn.ordinal > boundaryOrdinal)
    .sort((a, b) => a.ordinal - b.ordinal);
  const boundary = discarded[0];
  if (boundary === undefined) return null;
  const ref = boundary.nativeTurnRef;
  if (ref === null || ref.strength !== "strong" || ref.nativeId === null) return undefined;
  return ref.nativeId;
}

/**
 * Human-readable output for one subagent-extension task result: the last
 * assistant text from its transcript, or the error/stderr when it failed.
 */
function piSubagentOutput(result: unknown): string {
  const stopReason = recordString(result, "stopReason");
  const failed =
    (recordNumber(result, "exitCode") ?? 0) !== 0 ||
    stopReason === "error" ||
    stopReason === "aborted";
  if (failed) {
    // Falsy fallback, not `??`: an empty `errorMessage` must not suppress a
    // non-empty `stderr`, which is often the only description of the failure.
    const failure = recordString(result, "errorMessage") || recordString(result, "stderr");
    if (failure !== undefined && failure.length > 0) return failure;
  }
  const messages = recordField(result, "messages");
  if (!Array.isArray(messages)) return "";
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (recordString(message, "role") !== "assistant") continue;
    const text = contentText(recordField(message, "content"));
    if (text.length > 0) return text;
  }
  return "";
}

function piExtensionDisplayName(extensionPath: string | undefined): string {
  if (extensionPath === undefined) return "Pi extension";
  const normalized = extensionPath.replace(/\\/g, "/").replace(/\/+$/, "");
  const name = normalized.slice(normalized.lastIndexOf("/") + 1).replace(/\.[^.]+$/, "");
  return name.length === 0 ? "Pi extension" : name;
}

function piThreadSnapshot(
  providerThread: OrchestrationV2ProviderThread,
): ProviderAdapter.ProviderAdapterV2ThreadSnapshot {
  return { providerThread, providerTurns: [], messages: [], runtimeRequests: [] };
}

function piQuestion(
  questionId: string,
  method: "select" | "input" | "editor",
  title: string,
  event: PiRpcRecord,
): OrchestrationV2UserInputQuestion {
  const options =
    method === "select" && Array.isArray(event["options"])
      ? event["options"]
          .filter((option): option is string => typeof option === "string")
          .map((option) => ({ label: option || "Empty value", description: option, value: option }))
      : [
          {
            label: "Submit empty value",
            description: "Send an empty string to the extension.",
            value: "",
          },
        ];
  // The user-input contract has no prefill field, so an editor dialog's
  // prefill is surfaced inside the question text; without it the user would
  // edit blind against content they cannot see.
  const prefill = method === "editor" ? recordString(event, "prefill") : undefined;
  const question = recordString(event, "message") ?? recordString(event, "placeholder") ?? title;
  return {
    id: questionId,
    header: title,
    question:
      prefill === undefined || prefill.length === 0
        ? question
        : `${question}\n\nCurrent value:\n${prefill.slice(0, 2_000)}`,
    options,
  };
}

function piUiResponse(
  pending: PendingPiPrompt,
  decision: ProviderApprovalDecision | undefined,
  answers: Record<string, unknown> | undefined,
): PiRpcRecord {
  if (pending.method === "confirm") {
    if (decision === "accept" || decision === "acceptForSession") return { confirmed: true };
    if (decision === "decline") return { confirmed: false };
    return { cancelled: true };
  }
  const answer = answers?.[pending.questionId];
  // An empty string is a valid dialog value per the RPC spec (the extension
  // receives ""), distinct from cancelling (the extension receives undefined).
  if (typeof answer === "string") return { value: answer };
  return { cancelled: true };
}

// ── driver ────────────────────────────────────────────────────

export type PiAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | ServerConfig.ServerConfig;

/** Both flavors share one settings shape; only defaults and form copy differ. */
function makePiFlavorAdapterV2Driver(
  flavor: PiFlavor,
  configSchema: typeof PiSettings | typeof PrimeAgentSettings,
  defaultConfig: PiSettings,
): ProviderAdapterDriver<PiSettings, PiAdapterV2DriverEnv> {
  return {
    driverKind: flavor.driverKind,
    configSchema,
    defaultConfig: (): PiSettings => defaultConfig,
    create: Effect.fn("PiAdapterV2Driver.create")(
      function* (input: ProviderAdapterDriverCreateInput<PiSettings>) {
        const hostEnvironment = yield* HostProcessEnvironment;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const fileSystem = yield* FileSystem.FileSystem;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const serverConfig = yield* ServerConfig.ServerConfig;
        const continuationRequests =
          yield* ProviderContinuationRequests.ProviderContinuationRequests;
        return makePiAdapterV2({
          flavor,
          continuationRequests,
          instanceId: input.instanceId,
          settings: { ...input.config, enabled: input.enabled },
          environment: mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
          spawner,
          fileSystem,
          idAllocator,
          serverConfig,
        });
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterDriverCreateError({
                driver: flavor.driverKind,
                instanceId: input.instanceId,
                detail: `Failed to create ${flavor.displayName} adapter.`,
                cause,
              }),
          ),
        ),
    ),
  };
}

export const PiAdapterV2Driver = makePiFlavorAdapterV2Driver(
  PI_FLAVOR,
  PiSettings,
  DEFAULT_PI_SETTINGS,
);
export const PrimeAgentAdapterV2Driver = makePiFlavorAdapterV2Driver(
  PRIME_AGENT_FLAVOR,
  PrimeAgentSettings,
  Schema.decodeSync(PrimeAgentSettings)({}),
);

const layer: Layer.Layer<ProviderAdapter.ProviderAdapterV2, never, PiAdapterV2DriverEnv> =
  Layer.effect(
    ProviderAdapter.ProviderAdapterV2,
    Effect.gen(function* () {
      const hostEnvironment = yield* HostProcessEnvironment;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const serverConfig = yield* ServerConfig.ServerConfig;
      return makePiAdapterV2({
        flavor: PI_FLAVOR,
        instanceId: PI_DEFAULT_INSTANCE_ID,
        settings: DEFAULT_PI_SETTINGS,
        environment: hostEnvironment,
        spawner,
        fileSystem,
        idAllocator,
        serverConfig,
      });
    }),
  );
