import * as NodeModule from "node:module";
import * as NodeVM from "node:vm";
import { assert, describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  PRIME_AGENT_DIALOG_COMMAND,
  PRIME_AGENT_DIALOG_EXTENSION_SOURCE,
  PRIME_AGENT_DIALOG_RESULT,
} from "./primeAgentDialogExtensionSource.ts";
import { daemonSocketFromLaunchArgs } from "./primeAgentChildDialogs.ts";
import { resolvePiLaunchArgs } from "./piT3McpInjection.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
type RecordValue = Record<string, unknown>;
type TestUi = {
  select: (
    title: string,
    options: ReadonlyArray<string>,
    opts?: { signal: AbortSignal; timeout?: number },
  ) => Promise<string | undefined>;
  confirm: (
    title: string,
    message: string,
    opts?: { signal: AbortSignal; timeout?: number },
  ) => Promise<boolean>;
  input: (
    title: string,
    placeholder?: string,
    opts?: { signal: AbortSignal; timeout?: number },
  ) => Promise<string | undefined>;
  editor: (title: string, prefill?: string) => Promise<string | undefined>;
  notify: (message: string, level: string) => void;
};
type Entry = { type: "custom"; customType: string; data: unknown };
type Context = { ui: TestUi; sessionManager: { getEntries: () => ReadonlyArray<Entry> } };
type Handler = (event: { reason: string }, ctx: Context) => Promise<void>;
type Command = { handler: (args: string, ctx: Context) => Promise<void> };

function makeBus() {
  const listeners = new Map<string, Set<(data: unknown) => void>>();
  return {
    on: (name: string, listener: (data: unknown) => void) => {
      const group = listeners.get(name) ?? new Set();
      group.add(listener);
      listeners.set(name, group);
      return () => {
        group.delete(listener);
      };
    },
    // Like Prime Agent's event bus, listeners run synchronously.
    emit: (name: string, data: unknown) => {
      for (const listener of listeners.get(name) ?? []) listener(data);
    },
  };
}

/** A daemon stand-in that follows stock DaemonClient semantics for the calls the extension makes. */
function makeDaemon(options: { connect?: Promise<void>; attachFails?: boolean } = {}) {
  const clients: Array<FakeClient> = [];
  const responses: Array<RecordValue> = [];
  const attachGates = new Map<string, Promise<void>>();
  class FakeClient {
    readonly listeners = new Set<(message: RecordValue) => void>();
    readonly closeListeners = new Set<(error: Error) => void>();
    readonly answered = Promise.withResolvers<RecordValue>();
    readonly attachSent = Promise.withResolvers<void>();
    readonly requests: Array<RecordValue> = [];
    connected = false;
    closed = false;
    attachedSession: string | undefined;
    readonly socketPath: string;
    constructor(socketPath: string) {
      this.socketPath = socketPath;
      clients.push(this);
    }
    get isConnected() {
      return this.connected;
    }
    onMessage(listener: (message: RecordValue) => void) {
      this.listeners.add(listener);
      return () => this.listeners.delete(listener);
    }
    onClose(listener: (error: Error) => void) {
      this.closeListeners.add(listener);
      return () => this.closeListeners.delete(listener);
    }
    async connect() {
      await options.connect;
      this.connected = true;
    }
    async request(command: RecordValue) {
      if (!this.connected) throw new Error(`Cannot send ${String(command["type"])}: not connected`);
      this.requests.push(command);
      if (command["type"] === "attach") {
        const session = String(command["activeSessionId"]);
        this.attachSent.resolve();
        await attachGates.get(session);
        if (options.attachFails) return { success: false, error: "Child closed" };
        this.attachedSession = session;
      }
      if (command["type"] === "extension_ui_response") {
        responses.push(command);
        this.answered.resolve(command);
      }
      return { success: true };
    }
    // Stock close() does not notify close listeners; only a lost socket does.
    close() {
      this.connected = false;
      this.closed = true;
    }
    loseSocket() {
      this.connected = false;
      for (const listener of this.closeListeners) listener(new Error("Daemon socket closed"));
    }
    dispatch(message: RecordValue) {
      for (const listener of this.listeners) listener(message);
    }
  }
  /** The daemon writes a child dialog to every client attached with extension UI. */
  const broadcast = (message: RecordValue) => {
    for (const client of clients) {
      if (client.connected && client.attachedSession === message["activeSessionId"])
        client.dispatch(message);
    }
  };
  return { FakeClient, clients, responses, attachGates, broadcast };
}

const question = (activeSessionId: string, method = "confirm", id = "child-request-1") => ({
  type: "extension_ui_request",
  activeSessionId,
  id,
  method,
  payload: { title: "Question" },
});

async function loadExtension(
  options: {
    entries?: Array<Entry>;
    bus?: ReturnType<typeof makeBus>;
    daemon?: ReturnType<typeof makeDaemon>;
    connect?: Promise<void>;
    attachFails?: boolean;
  } = {},
) {
  const entries = options.entries ?? [];
  const bus = options.bus ?? makeBus();
  const daemon = options.daemon ?? makeDaemon(options);
  const { clients } = daemon;
  const commands = new Map<string, Command>();
  const handlers = new Map<string, Handler>();
  const source = NodeModule.stripTypeScriptTypes(
    PRIME_AGENT_DIALOG_EXTENSION_SOURCE.replace(/^import .*;$/gm, "").replace(
      "export default function",
      "function",
    ),
  );
  NodeVM.runInNewContext(`${source}\nchildDialogs(pi)`, {
    DaemonClient: daemon.FakeClient,
    defaultDaemonSocketPath: () => "/fixture/default.sock",
    AbortController,
    pi: {
      registerCommand: (name: string, command: Command) => commands.set(name, command),
      on: (name: string, handler: Handler) => handlers.set(name, handler),
      appendEntry: (customType: string, data: unknown) =>
        entries.push({ type: "custom", customType, data }),
      events: bus,
    },
  });
  const notices: Array<string> = [];
  const ctx: Context = {
    ui: {
      select: async () => "selected",
      confirm: async () => true,
      input: async () => "typed",
      editor: async () => "edited",
      notify: (message) => {
        notices.push(message);
      },
    },
    sessionManager: { getEntries: () => entries },
  };
  const attach = (context = ctx, activeSessionId = "child-1") =>
    commands.get(PRIME_AGENT_DIALOG_COMMAND)!.handler(
      encodeJson({
        requestId: `route-${activeSessionId}`,
        activeSessionId,
        socketPath: "/fixture/daemon.sock",
      }),
      context,
    );
  const dispatchDialog = (method: string, payload: RecordValue = {}) =>
    clients[0]!.dispatch({
      ...question("child-1", method),
      payload: { title: "Question", ...payload },
    });
  const lastResult = () => decodeJson(notices.at(-1)!.slice(PRIME_AGENT_DIALOG_RESULT.length));
  const reload = async () => {
    await handlers.get("session_shutdown")!({ reason: "reload" }, ctx);
    const next = await loadExtension({ entries, bus, daemon });
    return { next, started: next.handlers.get("session_start")!({ reason: "reload" }, next.ctx) };
  };
  return {
    clients,
    daemon,
    ctx,
    notices,
    entries,
    bus,
    attach,
    dispatchDialog,
    handlers,
    lastResult,
    reload,
  };
}

describe("Prime Agent child dialog extension", () => {
  it.each([
    { method: "select", expected: { value: "selected" } },
    { method: "confirm", expected: { confirmed: true } },
    { method: "input", expected: { value: "typed" } },
    { method: "editor", expected: { value: "edited" } },
  ])(
    "routes $method and returns the parent's exact answer to the original request",
    async ({ method, expected }) => {
      const fixture = await loadExtension();
      const titles: Array<string> = [];
      // Capture the real UI call without replacing the source's routing logic.
      fixture.ctx.ui.select = async (title, options, opts) => {
        titles.push(title);
        assert.deepEqual(options, ["selected"]);
        assert.equal(opts?.timeout, 123);
        return "selected";
      };
      fixture.ctx.ui.confirm = async (title, message, opts) => {
        titles.push(title);
        assert.equal(message, "Allow?");
        assert.equal(opts?.timeout, 123);
        return true;
      };
      fixture.ctx.ui.input = async (title, placeholder, opts) => {
        titles.push(title);
        assert.equal(placeholder, "hint");
        assert.equal(opts?.timeout, 123);
        return "typed";
      };
      fixture.ctx.ui.editor = async (title, prefill) => {
        titles.push(title);
        assert.equal(prefill, "draft");
        return "edited";
      };
      await fixture.attach();
      fixture.dispatchDialog(String(method), {
        options: ["selected"],
        message: "Allow?",
        placeholder: "hint",
        prefill: "draft",
        timeout: 123,
      });
      const answer = await fixture.clients[0]!.answered.promise;
      assert.equal(answer["activeSessionId"], "child-1");
      assert.equal(answer["requestId"], "child-request-1");
      assert.deepEqual(answer["response"], expected);
      assert.deepEqual(titles, ["[Child child-1] Question"]);
    },
  );

  it.each(["select", "input", "editor"])(
    "returns cancellation when the parent cancels %s",
    async (method) => {
      const fixture = await loadExtension();
      fixture.ctx.ui.select = async () => undefined;
      fixture.ctx.ui.input = async () => undefined;
      fixture.ctx.ui.editor = async () => undefined;
      await fixture.attach();
      fixture.dispatchDialog(method, { options: ["selected"] });
      assert.deepEqual((await fixture.clients[0]!.answered.promise)["response"], {
        cancelled: true,
      });
    },
  );

  it("never turns a declined confirmation into approval", async () => {
    const fixture = await loadExtension();
    fixture.ctx.ui.confirm = async () => false;
    await fixture.attach();
    fixture.dispatchDialog("confirm");
    assert.deepEqual((await fixture.clients[0]!.answered.promise)["response"], {
      confirmed: false,
    });
  });

  it("cancels when its captured parent context has been retired", async () => {
    const fixture = await loadExtension();
    const ctx = new Proxy(fixture.ctx, {
      get: (target, key) => {
        if (key === "ui") throw new Error("Retired parent context");
        return Reflect.get(target, key);
      },
    });
    // The ctx becomes stale after setup, as on reload/replacement.
    await fixture.attach();
    Object.defineProperty(fixture.ctx, "ui", { get: () => ctx.ui });
    fixture.dispatchDialog("confirm");
    assert.deepEqual((await fixture.clients[0]!.answered.promise)["response"], { cancelled: true });
  });

  it("cancels an outstanding question and closes its client on parent shutdown", async () => {
    const fixture = await loadExtension();
    const opened = Promise.withResolvers<void>();
    fixture.ctx.ui.confirm = (_title, _message, opts) =>
      new Promise<boolean>((_resolve, reject) => {
        opts?.signal.addEventListener("abort", () => reject(new Error("Parent shut down")), {
          once: true,
        });
        opened.resolve();
      });
    await fixture.attach();
    fixture.dispatchDialog("confirm");
    await opened.promise;
    await fixture.handlers.get("session_shutdown")!({ reason: "exit" }, fixture.ctx);
    assert.deepEqual((await fixture.clients[0]!.answered.promise)["response"], { cancelled: true });
    assert.isTrue(fixture.clients[0]!.closed);
  });

  it("gives a child question asked during reload one parent recipient and one answer", async () => {
    const first = await loadExtension();
    let staleUiCalls = 0;
    first.ctx.ui.confirm = async () => {
      staleUiCalls++;
      return true;
    };
    await first.attach(first.ctx, "child-a");
    await first.attach(first.ctx, "child-b");
    // A reattach of child-b would stall here, as in the reviewed interleaving.
    const stalled = Promise.withResolvers<void>();
    first.daemon.attachGates.set("child-b", stalled.promise);
    const { next, started } = await first.reload();
    let parentUiCalls = 0;
    next.ctx.ui.confirm = async () => {
      parentUiCalls++;
      return false;
    };
    first.daemon.broadcast(question("child-a"));
    stalled.resolve();
    await started;
    await first.clients[0]!.answered.promise;
    assert.equal(staleUiCalls + parentUiCalls, 1);
    assert.equal(parentUiCalls, 1);
    assert.deepEqual(
      first.daemon.responses.map((response) => response["response"]),
      [{ confirmed: false }],
    );
    // The reloaded extension owns the same attached clients; nothing was reattached or closed.
    assert.lengthOf(first.clients, 2);
    assert.isTrue(first.clients.every((client) => !client.closed));
  });

  it("returns the exact answer to a question opened before the parent reloaded", async () => {
    const first = await loadExtension();
    const typed = Promise.withResolvers<string | undefined>();
    first.ctx.ui.input = () => typed.promise;
    await first.attach();
    first.daemon.broadcast(question("child-1", "input"));
    const { started } = await first.reload();
    await started;
    typed.resolve("typed after reload");
    await first.clients[0]!.answered.promise;
    assert.deepEqual(
      first.daemon.responses.map((response) => response["response"]),
      [{ value: "typed after reload" }],
    );
  });

  it("closes adopted routes, cancelling open questions, when the reloaded parent exits", async () => {
    const first = await loadExtension();
    first.ctx.ui.confirm = (_title, _message, opts) =>
      new Promise<boolean>((_resolve, reject) =>
        opts?.signal.addEventListener("abort", () => reject(new Error("Parent shut down")), {
          once: true,
        }),
      );
    await first.attach();
    first.daemon.broadcast(question("child-1"));
    const { next, started } = await first.reload();
    await started;
    await next.handlers.get("session_shutdown")!({ reason: "exit" }, next.ctx);
    assert.deepEqual((await first.clients[0]!.answered.promise)["response"], { cancelled: true });
    assert.isTrue(first.clients[0]!.closed);
  });

  it("reattaches only routes whose daemon socket was lost when the parent reloads", async () => {
    const first = await loadExtension();
    await first.attach(first.ctx, "child-a");
    await first.attach(first.ctx, "child-b");
    first.clients[1]!.loseSocket();
    const { started } = await first.reload();
    await started;
    assert.deepEqual(
      first.clients.map((client) => [client.attachedSession, client.connected]),
      [
        ["child-a", true],
        ["child-b", false],
        ["child-b", true],
      ],
    );
  });

  it("skips a malformed saved route and still adopts live clients on reload", async () => {
    const first = await loadExtension();
    first.entries.push({ type: "custom", customType: "t3-child-dialog-route", data: {} });
    await first.attach();
    const { next, started } = await first.reload();
    await started;
    assert.lengthOf(first.clients, 1);
    first.daemon.broadcast(question("child-1"));
    assert.deepEqual((await first.clients[0]!.answered.promise)["response"], { confirmed: true });
    assert.deepEqual(next.notices, []);
  });

  it("forgets a route whose daemon socket closed, so the next attach really reconnects", async () => {
    const fixture = await loadExtension();
    const aborted = Promise.withResolvers<void>();
    fixture.ctx.ui.confirm = (_title, _message, opts) =>
      new Promise<boolean>(() =>
        opts?.signal.addEventListener("abort", () => aborted.resolve(), { once: true }),
      );
    await fixture.attach();
    fixture.daemon.broadcast(question("child-1"));
    fixture.clients[0]!.loseSocket();
    // The open parent question could no longer reach the child.
    await aborted.promise;
    fixture.ctx.ui.confirm = async () => true;
    await fixture.attach();
    assert.deepEqual(fixture.lastResult(), { requestId: "route-child-1", ok: true });
    assert.lengthOf(fixture.clients, 2);
    assert.equal(fixture.clients[1]!.attachedSession, "child-1");
    fixture.daemon.broadcast(question("child-1", "confirm", "child-request-2"));
    assert.deepEqual(await fixture.clients[1]!.answered.promise, {
      type: "extension_ui_response",
      activeSessionId: "child-1",
      requestId: "child-request-2",
      response: { confirmed: true },
    });
  });

  it("does not report readiness when the daemon socket closes during attach", async () => {
    const fixture = await loadExtension();
    const release = Promise.withResolvers<void>();
    fixture.daemon.attachGates.set("child-1", release.promise);
    const attached = fixture.attach();
    await fixture.clients[0]!.attachSent.promise;
    fixture.clients[0]!.loseSocket();
    release.resolve();
    await attached;
    assert.deepEqual(fixture.lastResult(), {
      requestId: "route-child-1",
      ok: false,
      error: "Error: The parent or child dialog bridge closed while attaching.",
    });
  });

  it("does not revive a closed child when the parent reloads", async () => {
    const first = await loadExtension();
    await first.attach();
    first.clients[0]!.dispatch({ type: "session_closed", activeSessionId: "child-1" });
    assert.isTrue(first.clients[0]!.closed);
    const { started } = await first.reload();
    await started;
    assert.lengthOf(first.clients, 1);
  });

  it("shares an in-flight attachment so duplicate commands do not create duplicate approvals", async () => {
    const connected = Promise.withResolvers<void>();
    const fixture = await loadExtension({ connect: connected.promise });
    const first = fixture.attach();
    const second = fixture.attach();
    assert.lengthOf(fixture.clients, 1);
    connected.resolve();
    await Promise.all([first, second]);
    fixture.dispatchDialog("confirm");
    assert.deepEqual((await fixture.clients[0]!.answered.promise)["response"], { confirmed: true });
  });

  it("does not report readiness when attaching fails", async () => {
    const fixture = await loadExtension({ attachFails: true });
    await fixture.attach();
    assert.isTrue(fixture.clients[0]!.closed);
    assert.deepEqual(fixture.lastResult(), {
      requestId: "route-child-1",
      ok: false,
      error: "Error: Child closed",
    });
  });
});

describe("daemonSocketFromLaunchArgs", () => {
  it("uses the last --daemon-socket, as Prime Agent's parser does", () => {
    const resolved = resolvePiLaunchArgs(
      "--daemon-socket /tmp/first.sock --model x --daemon-socket=/tmp/last.sock",
    );
    assert.isTrue(resolved.ok);
    assert.equal(daemonSocketFromLaunchArgs(resolved.ok ? resolved.args : []), "/tmp/last.sock");
    assert.isUndefined(daemonSocketFromLaunchArgs(["--model", "x"]));
  });
});
