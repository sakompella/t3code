import { DESKTOP_ATTACH_SECRET_NAME } from "@t3tools/shared/desktopBootstrapToken";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import { HttpClient, HttpClientResponse } from "effect/http";

export const ENVIRONMENT_ID = "env-home";
export const ATTACH_SECRET = Uint8Array.from({ length: 32 }, (_, index) => index);

export interface FakeServer {
  readonly up: boolean;
  readonly environmentId: string;
}

/** Answers the environment descriptor the way a server owning `environmentId` would. */
export const layerFakeServerHttp = (server: Ref.Ref<FakeServer>) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Ref.get(server).pipe(
        Effect.map((current) =>
          HttpClientResponse.fromWeb(
            request,
            current.up
              ? Response.json({ environmentId: current.environmentId, label: "home" })
              : new Response(null, { status: 503 }),
          ),
        ),
      ),
    ),
  );

/** Writes the files a server leaves in its state directory once it is running. */
export const writeServerState = Effect.fn(function* (
  stateDir: string,
  input: { readonly pid: number; readonly withAttachSecret?: boolean },
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fileSystem.makeDirectory(path.join(stateDir, "secrets"), { recursive: true });
  yield* fileSystem.writeFileString(
    path.join(stateDir, "server-runtime.json"),
    // What a launchd-managed server writes; fields the app does not read are included on purpose.
    `{"version":1,"pid":${input.pid},"port":3773,"origin":"http://127.0.0.1:3773","startedAt":"2026-10-29T00:00:00.000Z","serviceManaged":true}\n`,
  );
  yield* fileSystem.writeFileString(path.join(stateDir, "environment-id"), `${ENVIRONMENT_ID}\n`);
  if (input.withAttachSecret !== false) {
    yield* fileSystem.writeFile(
      path.join(stateDir, "secrets", `${DESKTOP_ATTACH_SECRET_NAME}.bin`),
      ATTACH_SECRET,
    );
  }
});
