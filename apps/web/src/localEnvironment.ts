import { PRIMARY_LOCAL_ENVIRONMENT_ID } from "@t3tools/contracts";

export function isLocalEnvironmentDisabled(): boolean {
  return window.desktopBridge?.getLocalEnvironmentEnabled() === false;
}

let localServerExternal: boolean | undefined;

/** True when the desktop app attaches to a local server it does not run. Fixed until relaunch. */
export function isLocalServerExternal(): boolean {
  localServerExternal ??=
    window.desktopBridge
      ?.getLocalEnvironmentBootstraps()
      .some(
        (entry) => entry.id === PRIMARY_LOCAL_ENVIRONMENT_ID && entry.externalServer === true,
      ) === true;
  return localServerExternal;
}
