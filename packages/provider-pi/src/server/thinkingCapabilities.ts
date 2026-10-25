import { type ModelCapabilities, type ProviderOptionChoice } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Predicate from "effect/Predicate";

// xhigh/max require per-model thinkingLevelMap opt-in; global advertising makes set_thinking_level fail.
const PI_THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;

export type PiThinkingLevel = (typeof PI_THINKING_LEVELS)[number];

const PI_THINKING_LEVEL_LABELS: Record<PiThinkingLevel, string> = {
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
};

export const EMPTY_PI_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

/**
 * `sessionLevels` are the levels the session may hold. The model's default is
 * the one they all clamp to, and there is none when they disagree.
 */
export function thinkingCapabilitiesForPiModel(
  model: unknown,
  sessionLevels: ReadonlyArray<string>,
): ModelCapabilities {
  const levels = supportedPiThinkingLevelsFromModel(model);
  if (levels.length === 0) return EMPTY_PI_MODEL_CAPABILITIES;
  const defaults = new Set(sessionLevels.map((level) => clampPiThinkingLevel(level, levels)));
  const [defaultLevel] = defaults.size === 1 ? defaults : [];
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinking",
        label: "Thinking",
        type: "select",
        options: levels.map((level): ProviderOptionChoice => ({
          id: level,
          label: PI_THINKING_LEVEL_LABELS[level],
          ...(level === defaultLevel ? { isDefault: true } : {}),
        })),
      },
    ],
  });
}

/**
 * The levels a session that remembers the level asked for may hold when
 * `get_state` reports `reported` on `activeModel`: every level that clamps to
 * it there.
 */
export function piThinkingLevelsReportedAs(
  reported: string,
  activeModel: unknown,
): ReadonlyArray<PiThinkingLevel> {
  const levels = supportedPiThinkingLevelsFromModel(activeModel);
  return PI_THINKING_LEVELS.filter((level) => clampPiThinkingLevel(level, levels) === reported);
}

function clampPiThinkingLevel(
  input: unknown,
  availableLevels: ReadonlyArray<PiThinkingLevel>,
): PiThinkingLevel | undefined {
  if (typeof input !== "string") return undefined;
  const requestedIndex = PI_THINKING_LEVELS.findIndex((level) => level === input);
  if (requestedIndex === -1) return undefined;
  const exact = availableLevels.find((level) => level === input);
  if (exact !== undefined) return exact;
  for (let index = requestedIndex + 1; index < PI_THINKING_LEVELS.length; index += 1) {
    const higher = availableLevels.find((level) => level === PI_THINKING_LEVELS[index]);
    if (higher !== undefined) return higher;
  }
  for (let index = requestedIndex - 1; index >= 0; index -= 1) {
    const lower = availableLevels.find((level) => level === PI_THINKING_LEVELS[index]);
    if (lower !== undefined) return lower;
  }
  return availableLevels[0];
}

function supportedPiThinkingLevelsFromModel(model: unknown): ReadonlyArray<PiThinkingLevel> {
  if (recordField(model, "reasoning") !== true) return [];
  const thinkingLevelMap = thinkingLevelMapFromModel(model);
  return PI_THINKING_LEVELS.filter((level) => {
    const mapped = thinkingLevelMap?.[level];
    if (mapped === null) return false;
    if (level === "xhigh" || level === "max") return mapped !== undefined;
    return true;
  });
}

function thinkingLevelMapFromModel(model: unknown): Record<string, unknown> | undefined {
  const value = recordField(model, "thinkingLevelMap");
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

function recordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? input[key] : undefined;
}
