import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Transformer } from "@napi-rs/image";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import { PROVIDER_IMAGE_MAX_SIDE_PX, readImageForProvider } from "./providerImage.ts";

// Screenshot-sized noise, so PNG and JPEG sizes are realistic.
const makeImage = (width: number, height: number, format: "png" | "jpeg") => {
  const pixels = new Uint8Array(width * height * 4);
  for (let i = 0; i < pixels.length; i += 4) {
    pixels[i] = (i * 7) % 251;
    pixels[i + 1] = (i * 13) % 241;
    pixels[i + 2] = 200;
    pixels[i + 3] = 255;
  }
  const image = Transformer.fromRgbaPixels(pixels, width, height);
  return Effect.promise(() => (format === "png" ? image.png() : image.jpeg(90)));
};

const dimensions = (bytes: Uint8Array) =>
  Effect.promise(() => new Transformer(bytes).metadata()).pipe(
    Effect.map(({ width, height, format }) => ({ width, height, format })),
  );

const sentToProvider = (stored: Uint8Array, mimeType: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const dir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-provider-image-" });
    const path = `${dir}/image`;
    yield* fileSystem.writeFile(path, stored);
    const sent = yield* readImageForProvider(path, mimeType);
    return { sent, onDisk: yield* fileSystem.readFile(path) };
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect.each([
  { format: "png", mimeType: "image/png" },
  { format: "jpeg", mimeType: "image/jpeg" },
] as const)(
  "scales a 3840x2486 $format screenshot to fit 2000 px, in the same format",
  ({ format, mimeType }) =>
    Effect.gen(function* () {
      const stored = yield* makeImage(3840, 2486, format);
      const { sent, onDisk } = yield* sentToProvider(stored, mimeType);

      assert.equal(sent.mimeType, mimeType);
      assert.deepEqual(yield* dimensions(sent.data), {
        width: PROVIDER_IMAGE_MAX_SIDE_PX,
        height: Math.round((2486 * PROVIDER_IMAGE_MAX_SIDE_PX) / 3840),
        format,
      });
      assert.deepEqual(onDisk, new Uint8Array(stored));
    }),
);

it.effect("scales by the long side of a portrait image", () =>
  Effect.gen(function* () {
    const { sent } = yield* sentToProvider(yield* makeImage(900, 4200, "png"), "image/png");
    const { width, height } = yield* dimensions(sent.data);
    assert.equal(height, PROVIDER_IMAGE_MAX_SIDE_PX);
    assert.equal(width, Math.round((900 * PROVIDER_IMAGE_MAX_SIDE_PX) / 4200));
  }),
);

it.effect("sends an image at the limit as stored", () =>
  Effect.gen(function* () {
    const stored = yield* makeImage(2000, 1500, "png");
    const { sent } = yield* sentToProvider(stored, "image/png");
    assert.deepEqual(sent, { data: new Uint8Array(stored), mimeType: "image/png" });
  }),
);

it.effect("sends bytes it cannot decode as stored", () =>
  Effect.gen(function* () {
    const garbage = new TextEncoder().encode("not an image");
    const { sent } = yield* sentToProvider(garbage, "image/png");
    assert.deepEqual(sent, { data: garbage, mimeType: "image/png" });
  }),
);
