import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Semaphore from "effect/Semaphore";
import type { OrchestrationV2SystemNoticeTone, ProviderDriverKind } from "@t3tools/contracts";
import {
  piRecordField as recordField,
  piRecordString as recordString,
  type PiRpcRecord,
} from "./PiRpc.ts";
import type { ActivePiTurn, PiItemHooks } from "./PiAdapterV2State.ts";

const FINISHING_UP_LABEL = "Finishing up…";
const FINISHED_UP_LABEL = "Finished up";
const FINISHING_UP_DELAY = Duration.millis(1_500);

export function makePrimeAgentSettle(input: {
  readonly enabled: boolean;
  readonly driver: ProviderDriverKind;
  readonly scope: Scope.Scope;
  readonly sessionEventPermit: Semaphore.Semaphore;
  readonly activeTurn: () => ActivePiTurn | null;
  readonly items: Pick<PiItemHooks, "emit" | "emitItemNode" | "baseItemFields">;
}) {
  const { enabled, driver, scope, sessionEventPermit, activeTurn } = input;
  const { emit, emitItemNode, baseItemFields } = input.items;
  /**
   * Notice ids are derived from the provider item id alone, so they need
   * the provider turn id: item ordinals restart at the same value in every
   * thread and attempt.
   */
  const nextNoticeId = (turn: ActivePiTurn, kind: string, key?: string) => {
    turn.noticeCount += 1;
    return `${turn.providerTurn.id}:${kind}:${key ?? turn.noticeCount}`;
  };

  /**
   * Routine events are `info`, and the live finishing-up row is `progress`;
   * only a failed refinement is a `warning`, so only it looks like one.
   */
  const emitNotice = Effect.fnUntraced(function* (
    turn: ActivePiTurn,
    nativeItemId: string,
    message: string,
    tone: OrchestrationV2SystemNoticeTone,
    status: "running" | "completed",
    startedAt: DateTime.Utc,
    emittedAt: DateTime.Utc,
  ) {
    const completedAt = status === "completed" ? emittedAt : null;
    yield* emitItemNode(turn, nativeItemId, "system", status, startedAt, completedAt);
    yield* emit({
      type: "turn_item.updated",
      driver,
      turnItem: {
        ...baseItemFields(turn, nativeItemId, startedAt, emittedAt),
        status,
        title: message,
        completedAt,
        type: "system_notice",
        message,
        tone,
      },
    });
  });

  /**
   * Prime Agent runs a model call for its own harness review after the
   * final reply and before `agent_end`, and says nothing while it does.
   * Without a row the turn looks like it is still thinking. Most turns
   * skip the review, so the row waits a moment before it appears.
   */
  const openFinishingUp = Effect.fnUntraced(function* (turn: ActivePiTurn) {
    if (!enabled || turn.finishingUp !== null || turn.interrupted) {
      return;
    }
    const startedAt = yield* DateTime.now;
    const finishingUp = {
      nativeItemId: nextNoticeId(turn, "finishing-up"),
      startedAt,
      shown: false,
    };
    turn.finishingUp = finishingUp;
    yield* Effect.sleep(FINISHING_UP_DELAY).pipe(
      Effect.andThen(
        sessionEventPermit.withPermits(1)(
          Effect.gen(function* () {
            if (turn.finishingUp !== finishingUp || activeTurn() !== turn) return;
            finishingUp.shown = true;
            const shownAt = yield* DateTime.now;
            yield* emitNotice(
              turn,
              finishingUp.nativeItemId,
              FINISHING_UP_LABEL,
              "progress",
              "running",
              startedAt,
              shownAt,
            );
          }),
        ),
      ),
      Effect.forkIn(scope),
    );
  });

  const closeFinishingUp = Effect.fnUntraced(function* (turn: ActivePiTurn) {
    const finishingUp = turn.finishingUp;
    if (finishingUp === null) return;
    turn.finishingUp = null;
    if (!finishingUp.shown) return;
    const completedAt = yield* DateTime.now;
    yield* emitNotice(
      turn,
      finishingUp.nativeItemId,
      FINISHED_UP_LABEL,
      "progress",
      "completed",
      finishingUp.startedAt,
      completedAt,
    );
  });

  const emitRefineOutcome = Effect.fnUntraced(function* (event: PiRpcRecord) {
    const turn = activeTurn();
    if (turn === null || !enabled) return;
    const emittedAt = yield* DateTime.now;
    if (event["type"] === "refine_complete") {
      const result = recordField(event, "result");
      const summary = recordString(result, "summary")?.trim();
      const nativeItemId = nextNoticeId(turn, "refine", recordString(result, "id"));
      const message =
        summary === undefined || summary.length === 0
          ? "Refined its harness."
          : `Refined its harness: ${summary}`;
      yield* emitNotice(turn, nativeItemId, message, "info", "completed", emittedAt, emittedAt);
      return;
    }
    const detail = recordString(event, "error")?.trim();
    const message =
      detail === undefined || detail.length === 0
        ? "Harness refinement failed."
        : `Harness refinement failed: ${detail.slice(0, 500)}`;
    yield* emitNotice(
      turn,
      nextNoticeId(turn, "refine-failed"),
      message,
      "warning",
      "completed",
      emittedAt,
      emittedAt,
    );
  });

  return { openFinishingUp, closeFinishingUp, emitRefineOutcome };
}
