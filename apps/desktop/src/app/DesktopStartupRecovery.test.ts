import { assert, it as effectIt } from "@effect/vitest";
import type { DesktopUpdateState } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { describe, expect, it } from "vite-plus/test";

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as ElectronShell from "../electron/ElectronShell.ts";
import * as ElectronTheme from "../electron/ElectronTheme.ts";
import * as ElectronWindow from "../electron/ElectronWindow.ts";
import * as DesktopUpdates from "../updates/DesktopUpdates.ts";
import { createInitialDesktopUpdateState } from "../updates/updateMachine.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopState from "./DesktopState.ts";
import { listen, resolveStartupRecoveryOffer } from "./DesktopStartupRecovery.ts";

const updateState = (patch: Partial<DesktopUpdateState>): DesktopUpdateState => ({
  ...createInitialDesktopUpdateState(
    "0.1.3",
    { hostArch: "arm64", appArch: "arm64", runningUnderArm64Translation: false },
    "latest",
  ),
  enabled: true,
  ...patch,
});

describe("resolveStartupRecoveryOffer", () => {
  it("offers a newer version before going back", () => {
    expect(
      resolveStartupRecoveryOffer(
        updateState({ status: "available", availableVersion: "0.1.4" }),
        "0.1.2",
      ),
    ).toEqual({ _tag: "Update", version: "0.1.4" });
  });

  it("offers a retained download after a later check fails", () => {
    expect(
      resolveStartupRecoveryOffer(
        updateState({ status: "error", errorContext: "check", downloadedVersion: "0.1.4" }),
        null,
      ),
    ).toEqual({ _tag: "Update", version: "0.1.4" });
  });

  it("waits for the update check before offering to go back", () => {
    expect(resolveStartupRecoveryOffer(updateState({ status: "checking" }), "0.1.2")).toEqual({
      _tag: "Checking",
    });
  });

  it("offers the last version that started when no update exists", () => {
    expect(resolveStartupRecoveryOffer(updateState({ status: "up-to-date" }), "0.1.2")).toEqual({
      _tag: "Reinstall",
      version: "0.1.2",
    });
  });

  it("does not offer an update that already failed to apply here", () => {
    expect(
      resolveStartupRecoveryOffer(
        updateState({
          status: "downloaded",
          downloadedVersion: "0.1.4",
          notAppliedVersion: "0.1.4",
        }),
        "0.1.2",
      ),
    ).toEqual({ _tag: "Reinstall", version: "0.1.2" });
  });

  it("falls back to the releases page when this version never started", () => {
    expect(resolveStartupRecoveryOffer(updateState({ status: "disabled" }), "0.1.3")).toEqual({
      _tag: "Releases",
    });
    expect(resolveStartupRecoveryOffer(updateState({ status: "up-to-date" }), null)).toEqual({
      _tag: "Releases",
    });
  });
});

type WindowEvent =
  | { readonly _tag: "created"; readonly title: string | undefined }
  | { readonly _tag: "closed" };

/** A main window is open; the recovery window records what happens to it. */
const layerRecoveryHost = (windowEvents: Queue.Queue<WindowEvent>, updateChecks: string[]) => {
  const recoveryWindow = (title: string | undefined) => {
    let destroyed = false;
    return {
      webContents: { setWindowOpenHandler: () => undefined, on: () => undefined },
      once: () => undefined,
      isDestroyed: () => destroyed,
      loadURL: () => Promise.resolve(),
      close: () => {
        destroyed = true;
        Queue.offerUnsafe(windowEvents, { _tag: "closed" });
      },
      title,
    } as unknown as Electron.BrowserWindow;
  };
  return Layer.mergeAll(
    DesktopState.layer,
    Layer.succeed(DesktopUpdates.DesktopUpdates, {
      subscribe: Effect.succeed({ latest: updateState({}), changes: Stream.never }),
      check: (reason: string) =>
        Effect.sync(() => {
          updateChecks.push(reason);
        }).pipe(Effect.andThen(Effect.never)),
    } as unknown as DesktopUpdates.DesktopUpdates["Service"]),
    Layer.effect(
      DesktopEnvironment.DesktopEnvironment,
      Effect.map(
        Path.Path,
        (path) =>
          ({
            path,
            stateDir: "/home/userdata",
            logDir: "/home/userdata/logs",
            appVersion: "0.1.3",
            platform: "darwin",
            processArch: "arm64",
          }) as unknown as DesktopEnvironment.DesktopEnvironment["Service"],
      ),
    ).pipe(Layer.provide(Path.layer)),
    Layer.succeed(ElectronApp.ElectronApp, {
      on: () => Effect.void,
      quit: Effect.void,
    } as unknown as ElectronApp.ElectronApp["Service"]),
    Layer.succeed(ElectronShell.ElectronShell, {} as ElectronShell.ElectronShell["Service"]),
    Layer.succeed(ElectronTheme.ElectronTheme, {
      shouldUseDarkColors: Effect.succeed(false),
    } as unknown as ElectronTheme.ElectronTheme["Service"]),
    Layer.succeed(ElectronWindow.ElectronWindow, {
      main: Effect.succeedSome({} as Electron.BrowserWindow),
      create: (options: Electron.BrowserWindowConstructorOptions) =>
        Queue.offer(windowEvents, { _tag: "created", title: options.title }).pipe(
          Effect.as(recoveryWindow(options.title)),
        ),
      reveal: () => Effect.void,
    } as unknown as ElectronWindow.ElectronWindow["Service"]),
    FileSystem.layerNoop({ writeFileString: () => Effect.void }),
  );
};

describe("listen", () => {
  effectIt.effect(
    "shows the waiting window over the main window while an external server is down",
    () => {
      const updateChecks: string[] = [];
      return Effect.gen(function* () {
        const windowEvents = yield* Queue.unbounded<WindowEvent>();
        yield* Effect.gen(function* () {
          const { primaryBackendStartup } = yield* DesktopState.DesktopState;
          yield* listen;
          yield* SubscriptionRef.set(primaryBackendStartup, { _tag: "Ready" });
          yield* SubscriptionRef.set(primaryBackendStartup, {
            _tag: "Failing",
            reason: "The server is not running.",
            output: "",
            externalServer: true,
          });
          assert.deepEqual(yield* Queue.take(windowEvents), {
            _tag: "created",
            title: "Waiting for the T3 server",
          });

          yield* SubscriptionRef.set(primaryBackendStartup, { _tag: "Ready" });
          assert.deepEqual(yield* Queue.take(windowEvents), { _tag: "closed" });
          // No other app version can start a server this app does not run.
          assert.deepEqual(updateChecks, []);
        }).pipe(Effect.provide(layerRecoveryHost(windowEvents, updateChecks)));
      });
    },
  );
});
