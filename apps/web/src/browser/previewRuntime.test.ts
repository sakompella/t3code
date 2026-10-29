import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";

const local = vi.hoisted(() => ({ isLocalServerExternal: (): boolean => false }));

vi.mock("~/env", () => ({ isElectron: true }));
vi.mock("~/localEnvironment", () => local);
vi.mock("~/previewStateStore", () => ({ isPreviewSupportedInRuntime: () => true }));

const { alternatePreviewRuntime, rendersServerTabNatively } = await import("./previewRuntime");

const primary = EnvironmentId.make("primary");
const serverTab = { runtime: "server", headless: false } as const;

describe("server tabs of the primary environment", () => {
  it("render in this window when this app runs the server", () => {
    local.isLocalServerExternal = () => false;
    expect(rendersServerTabNatively(primary, primary, serverTab)).toBe(true);
    expect(alternatePreviewRuntime(primary, primary, true, serverTab)).toBeNull();
  });

  it("stream from an external server, which cannot drive this window's webviews", () => {
    local.isLocalServerExternal = () => true;
    expect(rendersServerTabNatively(primary, primary, serverTab)).toBe(false);
    expect(alternatePreviewRuntime(primary, primary, true, serverTab)).toBe("desktop");
  });
});
