// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no timingSafeEqual.
import * as NodeCrypto from "node:crypto";
import { AuthSessionId } from "@t3tools/contracts";
import * as Redacted from "effect/Redacted";

import type { ServerConfig } from "../config.ts";

export const REUSABLE_DEV_SESSION_PREFIX = "dev-auth-";

export function resolveReusableDevAuth(
  config: Pick<ServerConfig["Service"], "mode" | "devUrl" | "devAuthToken">,
) {
  if (config.mode !== "web" || config.devUrl === undefined || config.devAuthToken === undefined) {
    return undefined;
  }
  const token = config.devAuthToken;
  if (Redacted.value(token).length === 0) {
    return undefined;
  }
  const hash = NodeCrypto.createHash("sha256").update(Redacted.value(token)).digest();
  const tokenId = hash.toString("hex");
  return {
    credential: Redacted.value(token),
    sessionId: AuthSessionId.make(`${REUSABLE_DEV_SESSION_PREFIX}${tokenId}`),
    cookieName: `t3_dev_session_${tokenId}`,
    matches: (credential: string) =>
      NodeCrypto.timingSafeEqual(hash, NodeCrypto.createHash("sha256").update(credential).digest()),
  };
}
