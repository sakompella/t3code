import type { ProviderImage } from "@t3tools/provider-core/server/ProviderHost";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

/**
 * Anthropic rejects a request that holds more than 20 images when any image has
 * a side over 2000 px, and every later request in the conversation fails the
 * same way because the history keeps resending the image.
 */
export const PROVIDER_IMAGE_MAX_SIDE_PX = 2000;

const LOSSY_QUALITY = 90;

/**
 * `undefined` when the image already fits. PNG, JPEG and WebP keep their
 * format; any other format comes back as PNG.
 */
const shrinkToMaxSide = (stored: Uint8Array) =>
  Effect.tryPromise(async (): Promise<ProviderImage | undefined> => {
    // Loaded on first use: a platform without the native binary keeps running
    // and sends images as stored.
    const { FastResizeFilter, ResizeFit, Transformer } = await import("@napi-rs/image");
    const { width, height, format } = await new Transformer(stored).metadata();
    if (Math.max(width, height) <= PROVIDER_IMAGE_MAX_SIDE_PX) return undefined;
    // Encoding drops EXIF, so its orientation is applied to the pixels first.
    const resized = new Transformer(stored).rotate().fastResize({
      width: PROVIDER_IMAGE_MAX_SIDE_PX,
      height: PROVIDER_IMAGE_MAX_SIDE_PX,
      filter: FastResizeFilter.Lanczos3,
      fit: ResizeFit.Inside,
    });
    switch (format) {
      case "jpeg":
        return { data: await resized.jpeg(LOSSY_QUALITY), mimeType: "image/jpeg" };
      case "webp":
        return { data: await resized.webp(LOSSY_QUALITY), mimeType: "image/webp" };
      default:
        return { data: await resized.png(), mimeType: "image/png" };
    }
  });

/**
 * A stored image as a provider should inline it: scaled down to fit
 * `PROVIDER_IMAGE_MAX_SIDE_PX` when its long side is longer. The stored file is
 * not changed. An image that cannot be decoded is sent as stored, so a format
 * the decoder lacks is no worse off than before.
 */
export const readImageForProvider = Effect.fn("readImageForProvider")(function* (
  path: string,
  mimeType: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const stored = yield* fileSystem.readFile(path);
  const shrunk = yield* shrinkToMaxSide(stored).pipe(
    Effect.catch((cause) =>
      Effect.logWarning("Could not scale an image attachment; sending it as stored", {
        path,
        cause,
      }).pipe(Effect.as(undefined)),
    ),
  );
  return shrunk ?? ({ data: stored, mimeType } satisfies ProviderImage);
});
