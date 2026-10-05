/** Prime Agent's public daemon and extension APIs route child dialogs through the parent RPC UI. */
export const PRIME_AGENT_DIALOG_EXTENSION_FILENAME = "prime-agent-t3-dialogs.ts";
export const PRIME_AGENT_DIALOG_COMMAND = "t3-route-child-dialogs";
export const PRIME_AGENT_DIALOG_RESULT = "t3-child-dialog-route:";

export const PRIME_AGENT_DIALOG_EXTENSION_SOURCE = `\
import { DaemonClient, defaultDaemonSocketPath } from "@earendil-works/pi-coding-agent";
import type { ExtensionAPI, ExtensionCommandContext, AgentConnectionExtensionUiResponse } from "@earendil-works/pi-coding-agent";

const COMMAND = "t3-route-child-dialogs";
const RESULT = "t3-child-dialog-route:";
const ENTRY = "t3-child-dialog-route";
const HANDOFF = "t3-child-dialog-route-handoff";

type Route = { readonly activeSessionId: string; readonly socketPath?: string };

function parseRoute(value: unknown): Route {
  if (typeof value !== "object" || value === null || !("activeSessionId" in value) || typeof value.activeSessionId !== "string" || value.activeSessionId.length === 0) {
    throw new Error("T3 child dialog routing requires an active session id.");
  }
  if ("socketPath" in value && value.socketPath !== undefined && typeof value.socketPath !== "string") throw new Error("Invalid daemon socket path.");
  return { activeSessionId: value.activeSessionId, ...("socketPath" in value && typeof value.socketPath === "string" ? { socketPath: value.socketPath } : {}) };
}

export default function childDialogs(pi: ExtensionAPI) {
  const routes = new Map<string, { client: DaemonClient; pending: Map<string, AbortController> }>();
  const connecting = new Map<string, Promise<void>>();
  let stopped = false;

  const closeRoutes = async () => {
    const previous = [...routes];
    routes.clear();
    await Promise.all(previous.map(async ([activeSessionId, route]) => {
      for (const [requestId, controller] of route.pending) {
        controller.abort();
        await route.client.request({ type: "extension_ui_response", activeSessionId, requestId, response: { cancelled: true } }).catch(() => undefined);
      }
      route.client.close();
    }));
  };

  const openRoute = async (route: Route, ctx: Pick<ExtensionCommandContext, "ui">) => {
    if (stopped) throw new Error("The parent dialog bridge has shut down.");
    if (routes.has(route.activeSessionId)) return;
    const client = new DaemonClient(route.socketPath ?? defaultDaemonSocketPath());
    const pending = new Map<string, AbortController>();
    let closed = false;
    client.onMessage((message) => {
      if (message.type === "session_closed" && message.activeSessionId === route.activeSessionId) {
        closed = true;
        routes.delete(route.activeSessionId);
        try { pi.appendEntry(ENTRY, { ...route, closed: true }); } catch { /* A retired parent cannot persist lifecycle entries. */ }
        for (const controller of pending.values()) controller.abort();
        client.close();
        return;
      }
      if (message.type !== "extension_ui_request" || message.activeSessionId !== route.activeSessionId || !["select", "confirm", "input", "editor"].includes(message.method)) return;
      const controller = new AbortController();
      pending.set(message.id, controller);
      const answer = async (): Promise<AgentConnectionExtensionUiResponse> => {
        const payload = message.payload;
        const title = \`[Child \${route.activeSessionId}] \` + (typeof payload.title === "string" ? payload.title : message.method);
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
      void answer().catch((error: unknown): AgentConnectionExtensionUiResponse => {
        try { ctx.ui.notify(\`Child dialog could not reach T3: \${String(error)}\`, "error"); } catch { /* The parent context may have been retired. */ }
        return { cancelled: true };
      }).then((response) => client.request({ type: "extension_ui_response", activeSessionId: route.activeSessionId, requestId: message.id, response })).catch(() => {
        // The child may have cancelled the request while its parent UI was open.
      }).finally(() => pending.delete(message.id));
    });
    try {
      await client.connect();
      const response = await client.request({ type: "attach", activeSessionId: route.activeSessionId, supportsExtensionUi: true, capabilities: ["extension_ui", "slim_attach"] });
      if (!response.success) throw new Error(response.error);
      if (stopped || closed) throw new Error("The parent or child dialog bridge closed while attaching.");
      routes.set(route.activeSessionId, { client, pending });
    } catch (error) { client.close(); throw error; }
  };

  const attach = (route: Route, ctx: Pick<ExtensionCommandContext, "ui">) => {
    const existing = connecting.get(route.activeSessionId);
    if (existing) return existing;
    const ready = openRoute(route, ctx).finally(() => connecting.delete(route.activeSessionId));
    connecting.set(route.activeSessionId, ready);
    return ready;
  };

  pi.registerCommand(COMMAND, {
    description: "T3 Code internal: route child dialogs to the parent UI.",
    handler: async (args, ctx) => {
      const input: unknown = JSON.parse(args);
      const requestId = typeof input === "object" && input !== null && "requestId" in input && typeof input.requestId === "string" ? input.requestId : "";
      try {
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
    // Keep a recipient during reload. A retired ctx cancels rather than hanging;
    // the new runner takes over only after its replacement routes are attached.
    const unsubscribe = pi.events.on(HANDOFF, () => { unsubscribe(); stopped = true; void closeRoutes(); });
  });
  pi.on("session_start", async (event, ctx) => {
    if (event.reason !== "reload") return;
    const saved = new Map<string, Route>();
    for (const entry of ctx.sessionManager.getEntries()) {
      if (entry.type !== "custom" || entry.customType !== ENTRY) continue;
      const route = parseRoute(entry.data);
      if (typeof entry.data === "object" && entry.data !== null && "closed" in entry.data && entry.data.closed === true) {
        saved.delete(route.activeSessionId);
      } else {
        saved.set(route.activeSessionId, route);
      }
    }
    for (const route of saved.values()) await attach(route, ctx).catch((error: unknown) => ctx.ui.notify(\`Child dialog routing unavailable: \${String(error)}\`, "error"));
    pi.events.emit(HANDOFF, undefined);
  });
}
`;
