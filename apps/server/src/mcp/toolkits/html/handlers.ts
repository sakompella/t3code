import {
  CommandId,
  htmlRenderTurnItemId,
  OrchestratorMcpFailure,
  type ProviderInstanceId,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import type { HtmlRenderReference } from "@t3tools/shared/htmlRender";
import * as Effect from "effect/Effect";

import * as HtmlRender from "../../../htmlRender/HtmlRender.ts";
import { callsT3ToolsFromKernel } from "../../../orchestration-v2/Adapters/PiFlavor.ts";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { type Caller, dispatchFailure, readMutationCaller } from "../../threadAccess.ts";
import { HtmlPreviewToolkit, HtmlRenderToolkit, type HtmlToolkit } from "./tools.ts";

const INVALID_PAGE_ERRORS = new Set([
  "HtmlRenderImagesNotFoundError",
  "HtmlRenderImageTooLargeError",
  "HtmlRenderPageTooLargeError",
]);

// Every HTML render error message is built on the server and tells the agent what to do next.
const toFailure = (error: { readonly _tag: string; readonly message: string }) =>
  new OrchestratorMcpFailure({
    code: INVALID_PAGE_ERRORS.has(error._tag) ? "invalid_request" : "orchestration_error",
    message: error.message,
  });

/**
 * A kernel caller's own tool item shows only its code, so the page needs an
 * item of its own. Other providers' `html_render` items already carry it.
 */
const needsOwnTimelineItem = (providerInstanceId: ProviderInstanceId) =>
  ProviderAdapterRegistry.ProviderAdapterRegistryV2.pipe(
    Effect.flatMap((adapters) => adapters.get(providerInstanceId)),
    Effect.map((adapter) => callsT3ToolsFromKernel(adapter.driver)),
    Effect.mapError(
      () =>
        new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The calling provider is no longer available.",
        }),
    ),
  );

/**
 * Records a page as its own `html_render` item in the run. A record that
 * committed but lost its reply still owns the page, so the stored item decides.
 */
const recordPage =
  (threads: Caller["threads"], threadId: ThreadId, runId: RunId) =>
  (published: HtmlRenderReference) => {
    const stored = threads
      .getTurnItem({ threadId, itemId: htmlRenderTurnItemId(published.attachmentId) })
      .pipe(
        Effect.map(({ item }) => item !== null),
        Effect.orElseSucceed(() => false),
      );
    return threads
      .dispatch({
        type: "html_render.record",
        commandId: CommandId.make(`mcp:html-render:${published.attachmentId}`),
        threadId,
        runId,
        htmlRender: published,
      })
      .pipe(
        Effect.asVoid,
        Effect.catch((error) =>
          stored.pipe(
            Effect.flatMap((kept) => (kept ? Effect.void : Effect.fail(dispatchFailure(error)))),
          ),
        ),
      );
  };

const handlers = {
  html_preview: (input) =>
    Effect.gen(function* () {
      // The headless browser runs on the host and can open local files, so
      // only agents T3 launched, which already work on this machine, get it.
      yield* McpInvocationContext.requireThreadScope(
        yield* McpInvocationContext.McpInvocationContext,
        "html_preview",
      );
      const htmlRender = yield* HtmlRender.HtmlRender;
      const { png, ...preview } = yield* htmlRender.preview(input).pipe(Effect.mapError(toFailure));
      return {
        ...preview,
        screenshot: {
          mimeType: "image/png" as const,
          data: png,
          width: preview.width,
          height: preview.capturedHeight,
        },
      };
    }),
  html_render: (input) =>
    Effect.gen(function* () {
      // The page is stored in the calling thread, so it needs that thread's
      // live run, like any other write.
      const { scope, caller, threads } = yield* readMutationCaller();
      const { thread } = yield* McpInvocationContext.requireThreadScope(scope, "html_render");
      const runId = caller?.activeRunId ?? null;
      const record =
        runId !== null && (yield* needsOwnTimelineItem(thread.providerInstanceId))
          ? recordPage(threads, thread.threadId, runId)
          : undefined;
      const htmlRender = yield* HtmlRender.HtmlRender;
      const reference = yield* htmlRender
        .publish({
          threadId: thread.threadId,
          ...input,
          ...(record === undefined ? {} : { record }),
        })
        .pipe(
          Effect.mapError((error) =>
            error._tag === "OrchestratorMcpFailure" ? error : toFailure(error),
          ),
        );
      return {
        htmlRender: reference,
        message:
          "Shown to the reader above your reply. Don't mention or describe the page; reply with only what it doesn't already say.",
      };
    }),
} satisfies Parameters<typeof HtmlToolkit.toLayer>[0];

export const layerPreview = HtmlPreviewToolkit.toLayer({
  html_preview: handlers.html_preview,
});

export const layerRender = HtmlRenderToolkit.toLayer({
  html_render: handlers.html_render,
});
