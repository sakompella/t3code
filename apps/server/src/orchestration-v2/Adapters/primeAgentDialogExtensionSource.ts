/** Prime Agent's public daemon and extension APIs route child dialogs through the parent RPC UI. */
export const PRIME_AGENT_DIALOG_EXTENSION_FILENAME = "prime-agent-t3-dialogs.ts";
export const PRIME_AGENT_DIALOG_COMMAND = "t3-route-child-dialogs";
export const PRIME_AGENT_DIALOG_RESULT = "t3-child-dialog-route:";

export const PRIME_AGENT_DIALOG_EXTENSION_SOURCE = `\
import { DaemonClient, defaultDaemonSocketPath } from "@earendil-works/pi-coding-agent";
import type { ExtensionAPI, ExtensionContext, AgentConnectionExtensionUiResponse } from "@earendil-works/pi-coding-agent";

const COMMAND = ${JSON.stringify(PRIME_AGENT_DIALOG_COMMAND)};
const RESULT = ${JSON.stringify(PRIME_AGENT_DIALOG_RESULT)};
const ENTRY = "t3-child-dialog-route";
const HANDOFF = "t3-child-dialog-route-handoff";

type Route = { readonly activeSessionId: string; readonly socketPath?: string };
type ReconnectStatus = Parameters<NonNullable<Parameters<DaemonClient["enableAutoReconnect"]>[0]["onStatus"]>>[0];
type LiveRoute = {
  readonly route: Route;
  readonly client: DaemonClient;
  /** Child requests open in the parent UI. */
  readonly pending: Map<string, AbortController>;
  /** Requests whose parent dialog closed while the socket was down; each gets cancelled:true once reattached. */
  readonly outbox: Set<string>;
  /** Only a ready route counts as attached. A reconnecting route is still owned and listened to. */
  ready: boolean;
  /** The current owner's failure handler. A reload moves the route to a new owner. */
  owner?: { readonly fail: (reason: string) => void };
};

/** Bounds stock DaemonClient recovery after a lost socket, as Prime Agent's own default does. */
const RECONNECT_TIMEOUT_MS = 60_000;
/** Takes ownership of a live route. Passed from a reloaded extension to the one it replaces. */
type Adopt = (route: LiveRoute) => void;

function parseRoute(value: unknown): Route {
  if (typeof value !== "object" || value === null || !("activeSessionId" in value) || typeof value.activeSessionId !== "string" || value.activeSessionId.length === 0) {
    throw new Error("T3 child dialog routing requires an active session id.");
  }
  if ("socketPath" in value && value.socketPath !== undefined && typeof value.socketPath !== "string") throw new Error("Invalid daemon socket path.");
  return { activeSessionId: value.activeSessionId, ...("socketPath" in value && typeof value.socketPath === "string" ? { socketPath: value.socketPath } : {}) };
}

export default function childDialogs(pi: ExtensionAPI) {
  // Owned routes. A ready route is attached; any other is reconnecting.
  const routes = new Map<string, { live: LiveRoute; release: () => void }>();
  const connecting = new Map<string, Promise<void>>();
  let stopped = false;
  let successor: Adopt | undefined;

  const closeRoutes = async () => {
    const previous = [...routes.values()];
    routes.clear();
    await Promise.all(previous.map(async ({ live: { route, client, pending } }) => {
      for (const [requestId, controller] of pending) {
        controller.abort();
        await client.request({ type: "extension_ui_response", activeSessionId: route.activeSessionId, requestId, response: { cancelled: true } }).catch(() => undefined);
      }
      client.close();
    }));
  };

  const attachRequest = (activeSessionId: string) =>
    ({ type: "attach", activeSessionId, supportsExtensionUi: true, capabilities: ["extension_ui", "slim_attach"] }) as const;

  /** Restores a route after stock auto-reconnect. Failures go to whoever owns the route by then. */
  const recover = (live: LiveRoute, status: ReconnectStatus) => {
    const { activeSessionId } = live.route;
    if (status.status === "failed") return live.owner?.fail(status.error);
    if (status.status !== "connected") return;
    void live.client.request(attachRequest(activeSessionId), undefined, { recoverable: false }).then(async (response) => {
      if (!response.success) return live.owner?.fail(response.error);
      if (!live.client.isConnected) return; // Lost again; the next reconnect retries.
      live.ready = true;
      for (const requestId of [...live.outbox]) {
        live.outbox.delete(requestId);
        await live.client.request({ type: "extension_ui_response", activeSessionId, requestId, response: { cancelled: true } }).catch(() => undefined);
      }
    }, () => {
      if (live.client.isConnected) live.owner?.fail("the child could not be reattached");
    });
  };

  /** Answers the route's dialogs through ctx until released. */
  const listen = (live: LiveRoute, ctx: Pick<ExtensionContext, "ui">) => {
    const { activeSessionId } = live.route;
    const drop = () => {
      if (routes.get(activeSessionId)?.live === live) routes.delete(activeSessionId);
      for (const controller of live.pending.values()) controller.abort();
      live.client.close();
    };
    const fail = (reason: string) => {
      drop();
      try { ctx.ui.notify(\`Child \${activeSessionId}: dialog routing to T3 was lost (\${reason}). Its questions can no longer be answered here.\`, "error"); } catch { /* The parent context may have been retired. */ }
    };
    // A lost socket cannot carry answers. Close the open parent dialogs and cancel them once reattached.
    const lose = () => {
      live.ready = false;
      for (const [requestId, controller] of live.pending) {
        live.outbox.add(requestId);
        controller.abort();
      }
    };
    live.owner = { fail };
    const stopMessages = live.client.onMessage((message) => {
      if (message.type === "session_closed" && message.activeSessionId === activeSessionId) {
        drop();
        try { pi.appendEntry(ENTRY, { ...live.route, closed: true }); } catch { /* A retired parent cannot persist lifecycle entries. */ }
        return;
      }
      if (message.type !== "extension_ui_request" || message.activeSessionId !== activeSessionId || !["select", "confirm", "input", "editor"].includes(message.method)) return;
      const controller = new AbortController();
      live.pending.set(message.id, controller);
      const answer = async (): Promise<AgentConnectionExtensionUiResponse> => {
        const payload = message.payload;
        const title = \`[Child \${activeSessionId}] \` + (typeof payload.title === "string" ? payload.title : message.method);
        const opts = { signal: controller.signal, ...(typeof payload.timeout === "number" ? { timeout: payload.timeout } : {}) };
        switch (message.method) {
          case "select": {
            if (!Array.isArray(payload.options) || !payload.options.every((option) => typeof option === "string")) throw new Error("Invalid child selection options.");
            const value = await ctx.ui.select(title, payload.options, opts);
            return value === undefined ? { cancelled: true } : { value };
          }
          case "confirm": return { confirmed: await ctx.ui.confirm(title, typeof payload.message === "string" ? payload.message : "", opts) };
          case "input": {
            const value = await ctx.ui.input(title, typeof payload.placeholder === "string" ? payload.placeholder : undefined, opts);
            return value === undefined ? { cancelled: true } : { value };
          }
          case "editor": {
            const value = await ctx.ui.editor(title, typeof payload.prefill === "string" ? payload.prefill : undefined);
            return value === undefined ? { cancelled: true } : { value };
          }
          default: return { cancelled: true };
        }
      };
      // An unavailable or retired parent must deny approval, never choose an answer.
      // The answer uses the route's client even after a reload hands it to a new owner.
      void answer().catch((error: unknown): AgentConnectionExtensionUiResponse => {
        try { ctx.ui.notify(\`Child dialog could not reach T3: \${String(error)}\`, "error"); } catch { /* The parent context may have been retired. */ }
        return { cancelled: true };
      }).then((response) => {
        // An aborted dialog's terminal response is sent by whoever aborted it (shutdown or the reconnect outbox).
        // The editor takes no signal, so its late answer must be dropped here.
        if (controller.signal.aborted) return;
        return live.client.request({ type: "extension_ui_response", activeSessionId, requestId: message.id, response });
      }).catch(() => {
        // The child may have cancelled the request while its parent UI was open.
      }).finally(() => live.pending.delete(message.id));
    });
    const stopClose = live.client.onClose(lose);
    return () => { stopMessages(); stopClose(); live.owner = undefined; };
  };

  const register = (live: LiveRoute, ctx: Pick<ExtensionContext, "ui">) => {
    if (successor) return successor(live);
    if (stopped || routes.has(live.route.activeSessionId)) {
      // A duplicate must not answer: its twin receives the same requests.
      for (const controller of live.pending.values()) controller.abort();
      live.client.close();
      return;
    }
    routes.set(live.route.activeSessionId, { live, release: listen(live, ctx) });
  };

  const openRoute = async (route: Route, ctx: Pick<ExtensionContext, "ui">) => {
    if (stopped) throw new Error("The parent dialog bridge has shut down.");
    const existing = routes.get(route.activeSessionId);
    if (existing?.live.ready) return;
    if (existing) throw new Error("The child dialog route is reconnecting to the Prime Agent daemon.");
    const live: LiveRoute = { route, client: new DaemonClient(route.socketPath ?? defaultDaemonSocketPath()), pending: new Map(), outbox: new Set(), ready: false };
    // Listen before attaching: the daemon may send a dialog right after the attach response.
    const release = listen(live, ctx);
    try {
      await live.client.connect();
      const response = await live.client.request(attachRequest(route.activeSessionId), undefined, { recoverable: false });
      if (!response.success) throw new Error(response.error);
      if (stopped || !live.client.isConnected) throw new Error("The parent or child dialog bridge closed while attaching.");
    } catch (error) {
      release();
      live.client.close();
      throw error;
    }
    release();
    live.ready = true;
    // The parent's own daemon connection owns daemon restarts; this client only reconnects to the same socket.
    live.client.enableAutoReconnect({ recoverDaemon: async () => {}, timeoutMs: RECONNECT_TIMEOUT_MS, onStatus: (status) => recover(live, status) });
    register(live, ctx);
  };

  const attach = (route: Route, ctx: Pick<ExtensionContext, "ui">) => {
    const existing = connecting.get(route.activeSessionId);
    if (existing) return existing;
    const ready = openRoute(route, ctx).finally(() => connecting.delete(route.activeSessionId));
    connecting.set(route.activeSessionId, ready);
    return ready;
  };

  pi.registerCommand(COMMAND, {
    description: "T3 Code internal: route child dialogs to the parent UI.",
    handler: async (args, ctx) => {
      let requestId = "";
      try {
        const input: unknown = JSON.parse(args);
        if (typeof input === "object" && input !== null && "requestId" in input && typeof input.requestId === "string") requestId = input.requestId;
        const route = parseRoute(input);
        await attach(route, ctx);
        pi.appendEntry(ENTRY, route);
        ctx.ui.notify(RESULT + JSON.stringify({ requestId, ok: true }), "info");
      } catch (error) {
        ctx.ui.notify(RESULT + JSON.stringify({ requestId, ok: false, error: String(error) }), "error");
      }
    },
  });

  pi.on("session_shutdown", async (event) => {
    if (event.reason !== "reload") { stopped = true; return closeRoutes(); }
    // Keep answering until the reloaded extension adopts these clients. Moving the
    // clients, not reattaching, keeps one parent recipient per child, and dialogs
    // already open here still send their answer through the moved client.
    const unsubscribe = pi.events.on(HANDOFF, (adopt) => {
      unsubscribe();
      if (typeof adopt !== "function") { stopped = true; void closeRoutes(); return; }
      const next = adopt as Adopt;
      successor = next;
      const previous = [...routes.values()];
      routes.clear();
      for (const { live, release } of previous) {
        release();
        next(live);
      }
    });
  });
  pi.on("session_start", async (event, ctx) => {
    if (event.reason !== "reload") return;
    // Adopt live clients first, so attaching saved routes cannot add a second recipient.
    const adopt: Adopt = (live) => register(live, ctx);
    pi.events.emit(HANDOFF, adopt);
    const saved = new Map<string, Route>();
    for (const entry of ctx.sessionManager.getEntries()) {
      if (entry.type !== "custom" || entry.customType !== ENTRY) continue;
      let route: Route;
      try { route = parseRoute(entry.data); } catch { continue; }
      if (typeof entry.data === "object" && entry.data !== null && "closed" in entry.data && entry.data.closed === true) {
        saved.delete(route.activeSessionId);
      } else {
        saved.set(route.activeSessionId, route);
      }
    }
    for (const route of saved.values()) await attach(route, ctx).catch((error: unknown) => ctx.ui.notify(\`Child dialog routing unavailable: \${String(error)}\`, "error"));
  });
}
`;
