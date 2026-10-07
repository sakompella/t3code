import { describe, expect, it } from "@effect/vitest";

import {
  type CacheMiss,
  type MessageSource,
  describeCacheMiss,
  emptyCacheMissState,
  observeCacheMiss,
  resetCacheBaseline,
  seedCacheMissState,
} from "./primeAgentCacheMiss.ts";

const MINUTE = 60_000;

interface Turn {
  readonly input?: number;
  readonly cacheRead?: number;
  readonly cacheWrite?: number;
  readonly output?: number;
  readonly totalTokens?: number;
  readonly model?: string;
  readonly responseId?: string;
}

/** Anthropic-like prices in $/token: input 3e-6, cache read 0.3e-6, cache write 3.75e-6. */
const reply = (turn: Turn, timestamp: number) => {
  const { input = 0, cacheRead = 0, cacheWrite = 0, output = 100 } = turn;
  return {
    role: "assistant",
    provider: "anthropic",
    model: turn.model ?? "claude",
    timestamp,
    ...(turn.responseId === undefined ? {} : { responseId: turn.responseId }),
    usage: {
      input,
      output,
      cacheRead,
      cacheWrite,
      totalTokens: turn.totalTokens ?? input + output + cacheRead + cacheWrite,
      cost: {
        input: input * 3e-6,
        cacheRead: cacheRead * 0.3e-6,
        cacheWrite: cacheWrite * 3.75e-6,
      },
    },
  };
};

const compaction = (timestamp?: number) => ({
  role: "compactionSummary",
  summary: "s",
  ...(timestamp === undefined ? {} : { timestamp }),
});

/** Counts each message in order and returns the misses found. */
const missesOf = (
  messages: ReadonlyArray<unknown>,
  source: MessageSource = "live",
): ReadonlyArray<CacheMiss> => {
  let state = emptyCacheMissState;
  const misses: Array<CacheMiss> = [];
  for (const message of messages) {
    const step = observeCacheMiss(state, message, source);
    state = step.state;
    if (step.miss !== undefined) misses.push(step.miss);
  }
  return misses;
};

const warm = reply({ input: 10, cacheWrite: 50_000 }, 0);

describe("observeCacheMiss", () => {
  it.each<[string, ReadonlyArray<unknown>, ReadonlyArray<Partial<CacheMiss>>]>([
    [
      "a healthy session",
      [warm, reply({ input: 10, cacheRead: 50_000, cacheWrite: 2_000 }, MINUTE)],
      [],
    ],
    [
      "a full miss after idle",
      [warm, reply({ input: 10, cacheWrite: 52_000 }, 10 * MINUTE)],
      [{ missedTokens: 50_010, idleMs: 10 * MINUTE, modelChanged: false }],
    ],
    [
      "a partial miss counts only the previous prompt",
      [warm, reply({ cacheRead: 30_000, cacheWrite: 40_000 }, MINUTE)],
      [{ missedTokens: 20_010 }],
    ],
    [
      "a miss under the noise floor",
      [warm, reply({ cacheRead: 49_500, cacheWrite: 600 }, MINUTE)],
      [],
    ],
    [
      "a model switch",
      [warm, reply({ input: 50_010, model: "other" }, MINUTE)],
      [{ missedTokens: 50_010, modelChanged: true }],
    ],
    [
      "a provider that never reports caching",
      [reply({ input: 50_000 }, 0), reply({ input: 52_000 }, MINUTE)],
      [],
    ],
    [
      "a total miss once the provider reported caching, then stopped",
      [warm, reply({ cacheRead: 50_010 }, MINUTE), reply({ input: 50_100 }, 2 * MINUTE)],
      [{ missedTokens: 50_010 }],
    ],
    [
      "an errored reply with zero tokens leaves the baseline alone",
      [warm, reply({ output: 0 }, MINUTE), reply({ input: 10, cacheRead: 50_000 }, 2 * MINUTE)],
      [],
    ],
    [
      "a compaction resets the baseline",
      [warm, compaction(MINUTE), reply({ input: 10, cacheWrite: 8_000 }, 2 * MINUTE)],
      [],
    ],
  ])("%s", (_name, messages, expected) => {
    expect(missesOf(messages)).toMatchObject(expected);
  });

  it("prices a miss at the paid rate minus the cache-read rate", () => {
    const [miss] = missesOf([warm, reply({ cacheRead: 1, cacheWrite: 50_010 }, MINUTE)]);
    expect(miss?.missedDollars).toBeCloseTo(50_009 * (3.75e-6 - 0.3e-6), 6);
  });

  it("treats an unknown cache-read rate as free", () => {
    const message = reply({ input: 50_010 }, MINUTE);
    const [miss] = missesOf([
      warm,
      { ...message, usage: { ...message.usage, cost: { input: 50_010 * 3e-6 } } },
    ]);
    expect(miss?.missedDollars).toBeCloseTo(50_010 * 3e-6, 6);
  });

  it("counts a message with no cost data without a price", () => {
    const { cost: _cost, ...usage } = reply({ input: 50_010 }, MINUTE).usage;
    const [miss] = missesOf([warm, { ...reply({}, MINUTE), usage }]);
    expect(miss).toMatchObject({ missedTokens: 50_010, missedDollars: 0 });
  });

  it("ignores messages it cannot read", () => {
    const malformed = [
      { role: "assistant", provider: "anthropic", model: "claude", timestamp: MINUTE },
      {
        role: "assistant",
        provider: "anthropic",
        model: "claude",
        timestamp: MINUTE,
        usage: { input: "many" },
      },
      { role: "user", content: "hi", timestamp: MINUTE },
      null,
      "text",
    ];
    expect(
      missesOf([warm, ...malformed, reply({ input: 10, cacheWrite: 52_000 }, 10 * MINUTE)]),
    ).toMatchObject([{ missedTokens: 50_010 }]);
  });

  describe("folded child usage", () => {
    const folded = reply(
      { input: 10, cacheRead: 90_000, cacheWrite: 1_000, totalTokens: 51_110 },
      MINUTE,
    );
    const afterFold = reply({ input: 10, cacheRead: 51_000, cacheWrite: 25_000 }, 2 * MINUTE);

    it("resets a stored message instead of counting it", () => {
      expect(missesOf([warm, folded, afterFold], "stored")).toEqual([]);
    });

    it("counts a live message at its own usage", () => {
      const own = reply({ input: 10, cacheWrite: 52_000 }, 10 * MINUTE);
      expect(missesOf([warm, own], "live")).toMatchObject([{ missedTokens: 50_010 }]);
    });

    it("trusts a live message even when its total is smaller than its parts", () => {
      const own = {
        ...reply({ input: 10, cacheWrite: 52_000 }, 10 * MINUTE),
        usage: { ...reply({ input: 10, cacheWrite: 52_000 }, 0).usage, totalTokens: 1 },
      };
      expect(missesOf([warm, own], "live")).toMatchObject([{ missedTokens: 50_010 }]);
      expect(missesOf([warm, own], "stored")).toEqual([]);
    });

    it("does not reset the baseline when the stored copy of a live message arrives", () => {
      let state = observeCacheMiss(emptyCacheMissState, warm, "live").state;
      const live = reply(
        { input: 10, cacheRead: 50_000, cacheWrite: 2_000, responseId: "r1" },
        MINUTE,
      );
      state = observeCacheMiss(state, live, "live").state;
      const storedCopy = {
        ...live,
        usage: { ...live.usage, input: 10, cacheRead: 90_000, totalTokens: 52_110 },
      };
      state = observeCacheMiss(state, storedCopy, "stored").state;
      const next = observeCacheMiss(
        state,
        reply({ input: 10, cacheWrite: 53_000 }, 20 * MINUTE),
        "live",
      );
      expect(next.miss).toMatchObject({ missedTokens: 52_010 });
    });
  });

  describe("counting each message once", () => {
    it("ignores a message that arrives again", () => {
      const miss = reply({ input: 10, cacheWrite: 52_000, responseId: "r1" }, 10 * MINUTE);
      const first = observeCacheMiss(
        observeCacheMiss(emptyCacheMissState, warm, "live").state,
        miss,
        "live",
      );
      expect(first.miss).toBeDefined();
      const again = observeCacheMiss(first.state, miss, "stored");
      expect(again.miss).toBeUndefined();
      expect(again.state).toBe(first.state);
    });

    it("counts distinct messages that share a timestamp", () => {
      const seeded = observeCacheMiss(emptyCacheMissState, warm, "live").state;
      const a = reply({ input: 10, cacheWrite: 52_000, responseId: "a" }, 10 * MINUTE);
      const b = reply(
        { input: 10, cacheRead: 100, cacheWrite: 60_000, responseId: "b" },
        10 * MINUTE,
      );
      const first = observeCacheMiss(seeded, a, "live");
      const second = observeCacheMiss(first.state, b, "live");
      expect(first.identity).not.toBe(second.identity);
      expect(first.miss).toMatchObject({ missedTokens: 50_010 });
      expect(second.miss).toMatchObject({ missedTokens: 52_010 - 100 });
    });

    it("keeps a message older than what was counted out of the baseline", () => {
      const older = reply({ input: 10, cacheWrite: 5_000, responseId: "older" }, MINUTE);
      const newer = reply({ input: 10, cacheRead: 50_000, cacheWrite: 2_000 }, 2 * MINUTE);
      let state = observeCacheMiss(emptyCacheMissState, warm, "live").state;
      state = observeCacheMiss(state, newer, "live").state;
      const late = observeCacheMiss(state, older, "stored");
      expect(late.miss).toBeUndefined();
      expect(late.state).toBe(state);
    });
  });

  describe("compaction and branch summaries", () => {
    it("ignore kept messages that precede the summary", () => {
      const kept = reply({ input: 10, cacheWrite: 90_000 }, MINUTE);
      const afterCompaction = reply({ input: 10, cacheWrite: 8_000 }, 3 * MINUTE);
      // The stored context lists the summary first, then the messages kept across it.
      expect(missesOf([compaction(2 * MINUTE), warm, kept, afterCompaction], "stored")).toEqual([]);
    });

    it("do not undo a reset taken live when the stored summary arrives late", () => {
      let state = observeCacheMiss(emptyCacheMissState, warm, "live").state;
      state = resetCacheBaseline(state);
      state = observeCacheMiss(
        state,
        reply({ input: 10, cacheWrite: 8_000 }, 3 * MINUTE),
        "live",
      ).state;
      state = observeCacheMiss(state, compaction(2 * MINUTE), "stored").state;
      const next = observeCacheMiss(
        state,
        reply({ input: 10, cacheWrite: 8_000 }, 20 * MINUTE),
        "live",
      );
      expect(next.miss).toMatchObject({ missedTokens: 8_010 });
    });

    it("reset on a branch summary", () => {
      const branch = { role: "branchSummary", summary: "s", timestamp: MINUTE };
      expect(missesOf([warm, branch, reply({ input: 10, cacheWrite: 8_000 }, 2 * MINUTE)])).toEqual(
        [],
      );
    });
  });
});

describe("seedCacheMissState", () => {
  it("leaves the baseline at the last reply, so the next one is compared with it", () => {
    const seeded = seedCacheMissState([
      warm,
      reply({ input: 10, cacheRead: 50_000, cacheWrite: 2_000 }, MINUTE),
    ]);
    const idle = observeCacheMiss(
      seeded,
      reply({ input: 10, cacheWrite: 53_000 }, 30 * MINUTE),
      "live",
    );
    expect(idle.miss).toMatchObject({ missedTokens: 52_010, idleMs: 29 * MINUTE });
  });

  it("keeps the sticky cache report across history", () => {
    const seeded = seedCacheMissState([warm, reply({ input: 50_000 }, MINUTE)]);
    expect(seeded.baseline?.reportedCache).toBe(true);
  });

  it("starts empty after a folded or summary message", () => {
    expect(seedCacheMissState([warm, compaction(MINUTE)]).baseline).toBeUndefined();
    const folded = reply({ input: 10, cacheRead: 90_000, totalTokens: 51_110 }, MINUTE);
    expect(seedCacheMissState([warm, folded]).baseline).toBeUndefined();
  });
});

describe("describeCacheMiss", () => {
  it.each<[string, CacheMiss, string | undefined]>([
    [
      "small and cheap",
      { missedTokens: 5_000, missedDollars: 0.02, idleMs: 0, modelChanged: false },
      undefined,
    ],
    [
      "idle past the cache lifetime",
      { missedTokens: 45_000, missedDollars: 0.14, idleMs: 7 * MINUTE, modelChanged: false },
      "Cache miss after 7m idle: 45k tokens re-billed (~$0.14)",
    ],
    [
      "a model switch wins over idle",
      { missedTokens: 20_000, missedDollars: 0, idleMs: 9 * MINUTE, modelChanged: true },
      "Cache miss after model switch: 20k tokens re-billed",
    ],
    [
      "costly but few tokens",
      { missedTokens: 8_000, missedDollars: 0.12, idleMs: MINUTE, modelChanged: false },
      "Cache miss: 8k tokens re-billed (~$0.12)",
    ],
  ])("%s", (_name, miss, text) => {
    expect(describeCacheMiss(miss)).toBe(text);
  });
});
