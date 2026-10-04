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
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserInputQuestion,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
  type OrchestrationV2ProviderTurnTokenUsage,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
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
  materializePiT3Skill,
  resolvePiLaunchArgs,
} from "./piT3McpInjection.ts";
import {
  PI_FILE_CHANGE_TOOLS,
  T3_NAVIGATE_TREE_COMMAND,
  T3_NAVIGATE_TREE_RESULT_MARKER,
} from "./piT3McpExtensionSource.ts";
import { PI_FLAVOR, PRIME_AGENT_FLAVOR, type PiFlavor } from "./PiFlavor.ts";
import { resolveKernelMcpAccess } from "./primeAgentT3Mcp.ts";

import type {
  ActivePiTurn,
  PiStreamItemState,
  PiCompactionStatus,
  PiCompactionState,
  PiProviderRetryState,
  PiTurnTreeRefs,
  PendingPiPrompt,
  PendingPiWake,
  PiItemScope,
  PiItemSink,
  PiThreadState,
} from "./PiAdapterV2State.ts";
import { makePrimeAgentChildThreads } from "./primeAgentChildThreads.ts";
import { primeAgentHeartbeats } from "./primeAgentHeartbeats.ts";
import { makePrimeAgentStream, snapshotBlock } from "./primeAgentStream.ts";
import { makePrimeAgentTools } from "./primeAgentTools.ts";
import { makePrimeAgentSettle } from "./primeAgentSettle.ts";
import {
  isPiWakeEvent,
  piWakeNotification,
  piWakeTrigger,
  makePrimeAgentBackgroundJobs,
  makePrimeAgentWakeNotices,
} from "./primeAgentWakes.ts";

export const PI_PROVIDER = PI_FLAVOR.driverKind;
const PI_DEFAULT_INSTANCE_ID = defaultInstanceIdForDriver(PI_PROVIDER);
const DEFAULT_PI_SETTINGS = Schema.decodeSync(PiSettings)({});

/**
 * Sentinel model slug meaning "do not call set_model": Pi resolves the model
 * from the user's own settings.json (`defaultProvider`/`defaultModel`).
 */
const PI_INHERIT_MODEL_SLUG = "default";

const STREAM_FLUSH_MS = 50;
const PI_REQUEST_TIMEOUT_MS = 15_000;
// Session lifecycle hooks reload extensions, MCP servers and language servers.
const PI_SESSION_TIMEOUT_MS = 60_000;
const PI_SKILL_DISCOVERY_TIMEOUT_MS = 4_000;
const SETTLE_PROBE_MAX_ATTEMPTS = 3;
const SETTLE_PROBE_RETRY_DELAY = Duration.millis(100);
/** Idle-probe flavors re-read state while a retry, compaction, or queued action is still running. */
const BUSY_PROBE_INITIAL_DELAY_MS = 100;
const BUSY_PROBE_MAX_DELAY_MS = 1_000;
/**
 * How often an idle-probe flavor checks for a turn that went silent. Its RPC
 * drops events without a trace when the daemon socket backs up, and `agent_end`
 * is among them, so a quiet turn is confirmed against `get_state`.
 */
const QUIET_TURN_PROBE_INTERVAL = Duration.seconds(15);
/** A busy re-probe chain logs its first poll and then one in this many. */
const BUSY_PROBE_LOG_EVERY = 30;

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
    subagents: {
      ...PiProviderCapabilitiesV2.subagents,
      // Observed `rlm_child_update` children get their own T3 thread.
      exposesSubagentThreadIds: flavor.childThreads,
    },
  };
}

const PI_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

// ── per-session state ─────────────────────────────────────────

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

// ── adapter ───────────────────────────────────────────────────

export function makePiAdapterV2(
  options: PiAdapterV2Options,
): ProviderAdapter.ProviderAdapterV2Shape {
  const { idAllocator, flavor } = options;
  const driver = flavor.driverKind;
  const name = flavor.displayName;
  const binary = options.settings.binaryPath || flavor.defaultBinary;
  const capabilities = piProviderCapabilities(flavor);
  /** The undeclared-MCP hint is shown once per adapter, not on every session open. */
  let mcpSetupHintShown = false;
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
      // An agent with its own MCP client reaches T3 through the t3-code skill,
      // but only once the user declared the server in its settings. Otherwise
      // the extension keeps registering T3's tools and the user gets a hint.
      const kernelMcpAccess =
        mcpSession === undefined || flavor.kernelMcp === null
          ? undefined
          : yield* resolveKernelMcpAccess({
              displayName: name,
              kernelMcp: flavor.kernelMcp,
              environment: options.environment,
              endpoint: mcpSession.endpoint,
            }).pipe(Effect.provideService(FileSystem.FileSystem, options.fileSystem));
      const skillPath = kernelMcpAccess?.declared
        ? yield* provideCacheFs(materializePiT3Skill(options.serverConfig.providerStatusCacheDir))
        : undefined;
      const mcpSetupHint =
        kernelMcpAccess !== undefined && !kernelMcpAccess.declared
          ? kernelMcpAccess.hint
          : undefined;
      const resolvedLaunchArgs = resolvePiLaunchArgs(options.settings.launchArgs);
      if (!resolvedLaunchArgs.ok) {
        return yield* protocolError(resolvedLaunchArgs.message);
      }
      const launch = buildPiRpcLaunch({
        launchArgs: resolvedLaunchArgs.args,
        environment: options.environment,
        mcpSession,
        extensionPath,
        ...(skillPath === undefined ? {} : { skillPath }),
        runtimeMode: input.runtimePolicy.runtimeMode,
      });
      const connection: PiRpcConnection = yield* makePiRpcConnection({
        command: binary,
        args: launch.args,
        cwd,
        env: launch.env,
        terminationGrace: flavor.terminationGrace,
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
      const backgroundJobs = makePrimeAgentBackgroundJobs((patch) =>
        threadState === null ? Effect.void : updateProviderThread(threadState, patch),
      );
      const { trackBackgroundJobs, completeBackgroundJob } = backgroundJobs;
      const hasPendingBackgroundWork = () =>
        childThreads.hasLiveChildren() || backgroundJobs.hasPendingJobs() || pendingWake !== null;
      // Answering a dialog and terminalizing a turn both publish lifecycle
      // events. Pi can settle immediately after `extension_ui_response`, so
      // serialize the two paths to stop `turn.terminal` from overtaking the
      // dialog's own resolution updates.
      const sessionEventPermit = yield* Semaphore.make(1);
      let threadState: PiThreadState | null = null;
      let registrationAttempted = false;
      let lastNativeThreadId: string | null = null;
      /** Prime Agent's id for the open session; it keys the session's heartbeats. */
      let heartbeatSessionId: string | null = null;
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

      /**
       * The session's heartbeats, or undefined when the flavor has none or
       * the agent cannot say. A failed read keeps what clients already show.
       */
      const readHeartbeats = () => {
        const sessionId = heartbeatSessionId;
        if (!flavor.heartbeats || sessionId === null) return Effect.succeed(undefined);
        return request({ type: "list_heartbeats" }, 2_000).pipe(
          Effect.map((listing) => primeAgentHeartbeats(listing, sessionId)),
          Effect.orElseSucceed(() => undefined),
        );
      };

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

      const streamItemId = (turn: PiItemSink, contentIndex: number) =>
        `${itemScope(turn).idPrefix}:m${turn.messageOrdinal}:c${contentIndex}`;

      const streamItemFor = Effect.fnUntraced(function* (
        turn: PiItemSink,
        kind: PiStreamItemState["kind"],
        contentIndex: number,
      ) {
        const nativeItemId = streamItemId(turn, contentIndex);
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

      const { adoptMessageIdentity, adoptSnapshot, adoptRecordedMessages } = makePrimeAgentStream({
        lossyStream: flavor.lossyStream,
        streamItemFor,
        completeStreamItem,
        scheduleStreamFlush,
        findStreamItem: (turn, contentIndex) =>
          turn.streamItems.get(streamItemId(turn, contentIndex)),
        reemitStreamItem: (turn, item) => emitStreamItem(turn, item, !item.completed),
      });

      const ipython = makePrimeAgentTools({
        driver,
        workspaceRelativePath,
        items: { emit, emitItemNode, baseItemFields },
      });

      // ── tools ─────────────────────────────────────────────

      const emitToolItem = Effect.fnUntraced(function* (
        turn: PiItemSink,
        event: PiRpcRecord,
        phase: "start" | "update" | "end",
        /** Ends a tool whose end event never came, with the outcome the caller knows. */
        settledAs?: "completed" | "failed" | "interrupted",
      ) {
        const toolCallId = recordString(event, "toolCallId");
        const toolName = recordString(event, "toolName") ?? "tool";
        if (toolCallId === undefined) return;
        if (phase === "start") {
          turn.toolArgs.set(toolCallId, event["args"]);
        }
        if (phase === "end") {
          turn.openTools.delete(toolCallId);
        } else {
          turn.openTools.set(toolCallId, { ...turn.openTools.get(toolCallId), ...event });
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
        const status = settledAs
          ? settledAs
          : completed
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
          yield* ipython.emitCell(turn, {
            toolCallId,
            args,
            resultRecord,
            shared,
            outputText,
            completed,
            emittedAt,
          });
          // A child's cells run in its own kernel. A synthesized end says nothing
          // about what the cell did, and the kernel may be gone by now.
          if (completed && !isError && settledAs === undefined && "turnInput" in turn) {
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
       * Ends the tools whose end event never came, as `status`. Pi has already
       * handed their results to the model, so leaving them running would show
       * work that is long finished.
       */
      const settleOpenTools = Effect.fnUntraced(function* (
        turn: PiItemSink,
        status: "completed" | "failed" | "interrupted",
      ) {
        for (const event of Array.from(turn.openTools.values())) {
          yield* emitToolItem(
            turn,
            { ...event, type: "tool_execution_end", result: event["partialResult"] },
            "end",
            status,
          );
        }
      });

      /**
       * A lossy stream (see `PiFlavor.lossyStream`) can drop the end of a reply
       * and of its tool calls, and the turn then ends with items still open
       * and text cut short. Prime Agent's own record of the conversation has
       * the rest, so read it before the open items are completed. A failed or
       * slow read leaves the items as the stream left them.
       */
      const reconcileOpenWork = Effect.fnUntraced(function* (turn: ActivePiTurn) {
        const hasOpenStreamItem = Array.from(turn.streamItems.values()).some(
          (item) => !item.completed,
        );
        if (!flavor.lossyStream || (!hasOpenStreamItem && turn.openTools.size === 0)) return;
        const recorded = yield* request({ type: "get_messages" }, 2_000).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        const messages = recordField(recorded, "messages");
        if (!Array.isArray(messages)) return;
        yield* adoptRecordedMessages(turn, messages);
        for (const [toolCallId, openEvent] of Array.from(turn.openTools)) {
          const toolResult = messages.find(
            (message) =>
              recordString(message, "role") === "toolResult" &&
              recordString(message, "toolCallId") === toolCallId,
          );
          if (toolResult === undefined) continue;
          yield* emitToolItem(
            turn,
            {
              ...openEvent,
              type: "tool_execution_end",
              isError: recordField(toolResult, "isError") === true,
              result: {
                content: recordField(toolResult, "content"),
                details: recordField(toolResult, "details"),
              },
            },
            "end",
          );
        }
      });

      const childThreads = makePrimeAgentChildThreads({
        driver,
        instanceId: options.instanceId,
        name,
        childThreads: flavor.childThreads,
        sessionThread: () => ({
          threadId: threadState?.providerThread.appThreadId ?? input.threadId,
          providerThreadId: threadState?.providerThread.id ?? null,
        }),
        contentText,
        idAllocator,
        request,
        items: {
          emit,
          providerRef,
          baseItemFields,
          itemOrdinal,
          emitToolItem,
          adoptSnapshot,
          completeOpenStreamItems,
          settleOpenTools,
        },
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

      const emitMcpSetupHint = Effect.fnUntraced(function* (turn: ActivePiTurn, hint: string) {
        const emittedAt = yield* DateTime.now;
        const nativeItemId = `${turn.providerTurn.id}:t3-mcp-setup`;
        yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "completed",
            title: "T3 Code MCP setup",
            completedAt: emittedAt,
            type: "system_notice",
            message: hint,
            tone: "warning",
          },
        });
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

      const { openFinishingUp, closeFinishingUp, emitRefineOutcome } = makePrimeAgentSettle({
        enabled: flavor.settleSignal === "idle_probe",
        driver,
        scope,
        sessionEventPermit,
        activeTurn: () => threadState?.activeTurn ?? null,
        items: { emit, emitItemNode, baseItemFields },
      });

      const { emitMidRunWakeNotice } = makePrimeAgentWakeNotices({
        enabled: flavor.selfWakes === "continuation",
        driver,
        handleOf: backgroundJobs.handleOf,
        items: { emit, emitItemNode, baseItemFields },
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

      const finalizeTurn = Effect.fnUntraced(function* (state: PiThreadState, processAlive = true) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        const completedAt = yield* DateTime.now;
        if (processAlive) yield* reconcileOpenWork(turn);
        yield* completeOpenStreamItems(turn);
        yield* settleOpenTools(
          turn,
          turn.interrupted ? "interrupted" : turn.failure === null ? "completed" : "failed",
        );
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
          yield* childThreads.interrupt(turn);
        }
        const treeRefs =
          turn.stopTreeRefs !== undefined ? turn.stopTreeRefs : yield* captureTurnTreeRefs();
        const turnStartEntryId = turn.startEntryId ?? treeRefs?.turnStartEntryId ?? null;
        const tokenUsage = processAlive
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
        // The agent may have set, changed, or ended a heartbeat during the turn,
        // and a fired one has moved its next run. A Stop ends the session.
        const heartbeats = turn.interrupted ? undefined : yield* readHeartbeats();
        yield* updateProviderThread(state, {
          status: "idle",
          ...(treeRefs?.leafId == null
            ? {}
            : { nativeConversationHeadRef: providerRef(treeRefs.leafId) }),
          ...(heartbeats === undefined ? {} : { heartbeats }),
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
        quiet = false,
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
                quiet,
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
                busyPolls,
                quiet,
                probeFailed: true,
              }),
          }),
          Effect.ignore,
          Effect.forkIn(scope),
        );
      };

      /**
       * Records why a settle probe found the agent busy, or that it got no
       * answer. It gets a span of its own because only logs inside a span
       * reach the trace file.
       */
      const logBusySettleProbe = (
        turn: ActivePiTurn,
        data: unknown,
        probe: {
          readonly quiet: boolean;
          readonly busyPolls: number;
          readonly probeFailed: boolean;
        },
      ) => {
        const sessionActions = recordField(data, "sessionActions");
        const active = recordField(sessionActions, "active");
        return Effect.logInfo("orchestration-v2.pi-settle-probe-busy", {
          driver,
          providerSessionId: input.providerSessionId,
          providerTurnId: turn.providerTurn.id,
          quiet: probe.quiet,
          busyPolls: probe.busyPolls,
          probeFailed: probe.probeFailed,
          sessionEventCount: turn.sessionEventCount,
          settleWhenIdle: turn.settleWhenIdle,
          isStreaming: recordField(data, "isStreaming"),
          isCompacting: recordField(data, "isCompacting"),
          pendingMessageCount: recordNumber(data, "pendingMessageCount"),
          queuedCount: recordNumber(sessionActions, "queuedCount"),
          activeKind: recordString(active, "kind"),
          activePhase: recordString(active, "phase"),
        }).pipe(Effect.withSpan("PiAdapterV2.settleProbeBusy"));
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
          notification: piWakeNotification(wake.events, name, backgroundJobs.handleOf),
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
            if (turn !== null && recordString(event["message"], "role") !== "assistant") {
              yield* emitMidRunWakeNotice(turn, event["message"]);
            }
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
          case "rlm_child_update":
          case "observed_session_event":
          case "observed_session_closed":
            yield* childThreads.handleEvent(event, turn);
            return;
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
            const quiet = event["quiet"] === true;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            if (
              turn === null ||
              turn.providerTurn.id !== event["providerTurnId"] ||
              turn.settleProbeGeneration !== event["settleProbeGeneration"] ||
              (!settleAfterAgentActivity && turn.sawAgentActivity) ||
              // A quiet turn may have lost its compaction_end, and idle state
              // already rules out a compaction that is still running.
              (!quiet && turn.activeCompaction !== null)
            ) {
              return;
            }
            const busyPolls = Math.max(0, Math.trunc(recordNumber(event, "busyPolls") ?? 0));
            // Pi follows busy work with events that re-probe. Without
            // agent_settled, the end of a retry wait or a queued action has no
            // event of its own, so keep reading state until it goes idle.
            // New work bumps the generation, which retires this chain.
            const probeAgainLater = Effect.sleep(
              Duration.millis(
                Math.min(BUSY_PROBE_INITIAL_DELAY_MS * 2 ** busyPolls, BUSY_PROBE_MAX_DELAY_MS),
              ),
            ).pipe(
              Effect.andThen(scheduleSettleProbe(turn, settleAfterAgentActivity, 1, busyPolls + 1)),
              Effect.forkIn(scope),
            );
            if (probeFailed) {
              // The next quiet check asks again. A turn that is merely slow to
              // answer get_state must not cost the session its process.
              if (quiet) return;
              if (!settleAfterAgentActivity) {
                if (state !== null) yield* finalizeTurn(state);
                return;
              }
              // Prime Agent answers get_state through its daemon, which can
              // take seconds under load while the agent keeps working. A dead
              // process closes stdout, so an unanswered probe only means busy.
              if (flavor.settleSignal === "idle_probe") {
                if (busyPolls % BUSY_PROBE_LOG_EVERY === 0) {
                  yield* logBusySettleProbe(turn, undefined, { quiet, busyPolls, probeFailed });
                }
                yield* probeAgainLater;
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
            if (quiet || busyPolls % BUSY_PROBE_LOG_EVERY === 0) {
              yield* logBusySettleProbe(turn, data, { quiet, busyPolls, probeFailed });
            }
            // A quiet check is its own timer: it asks again after the next
            // silent interval, so a long tool call is not polled every second.
            if (quiet) return;
            if (flavor.settleSignal === "idle_probe") yield* probeAgainLater;
            return;
          }
          default:
            return;
        }
      });

      yield* Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(connection.events);
          yield* sessionEventPermit.withPermits(1)(
            Effect.suspend(() => {
              const active = threadState?.activeTurn;
              // Synthetic `t3.*` records are the adapter talking to itself.
              if (active != null && !String(event["type"]).startsWith("t3.")) {
                active.sessionEventCount += 1;
              }
              return handleSessionEvent(event);
            }),
          );
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
              yield* childThreads.interrupt();
              yield* backgroundJobs.clear();
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

      // The RPC can lose `agent_end` along with the rest of a reply's closing
      // events and never says so, so a turn that goes silent after real agent
      // activity is checked against get_state. A turn that is still running
      // answers busy and stays open.
      if (flavor.settleSignal === "idle_probe") {
        yield* Effect.gen(function* () {
          let lastSeen: { readonly turn: ActivePiTurn; readonly eventCount: number } | null = null;
          while (true) {
            yield* Effect.sleep(QUIET_TURN_PROBE_INTERVAL);
            const turn = threadState?.activeTurn ?? null;
            const previous = lastSeen;
            lastSeen = turn === null ? null : { turn, eventCount: turn.sessionEventCount };
            if (
              turn !== null &&
              turn.sawAgentActivity &&
              previous?.turn === turn &&
              previous.eventCount === turn.sessionEventCount
            ) {
              yield* scheduleSettleProbe(turn, true, 1, 0, true);
            }
          }
        }).pipe(Effect.forkIn(scope));
      }

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
          if (flavor.selfWakes === "continuation") {
            // Prime Agent disposes the replaced session without cancelling a
            // pending auto-retry. The retry would still fire later and send
            // the failed request again, on the old model.
            yield* request({ type: "abort_retry" }, 2_000).pipe(Effect.ignore);
          }
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
        heartbeatSessionId = recordString(stateData, "sessionId") ?? null;
        // A persisted list says what ran before this process; read it afresh.
        const heartbeats = flavor.heartbeats ? ((yield* readHeartbeats()) ?? []) : undefined;
        const createdAt = yield* DateTime.now;
        const providerThread: OrchestrationV2ProviderThread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                nativeThreadRef: providerRef(nativeId),
                ...(needsNewSession ? { nativeConversationHeadRef: null, contextUsage: null } : {}),
                // A persisted roster outlives the process that ran those jobs,
                // and nothing would ever report them finished.
                ...(flavor.tools === "ipython"
                  ? { pendingBackgroundTasks: backgroundJobs.tasks() }
                  : {}),
                ...(heartbeats === undefined ? {} : { heartbeats }),
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
                ...(heartbeats === undefined ? {} : { heartbeats }),
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
              openTools: new Map(),
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
              sessionEventCount: 0,
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
              if (mcpSetupHint !== undefined && !mcpSetupHintShown) {
                mcpSetupHintShown = true;
                yield* emitMcpSetupHint(activeTurn, mcpSetupHint);
              }
              if (adoptedWake !== null) {
                if (isWakeContinuation)
                  activeTurn.wakeTrigger = piWakeTrigger(adoptedWake.events)?.message;
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
              childThreads.hasLiveChildren()
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
                    yield* reconcileOpenWork(turn);
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
