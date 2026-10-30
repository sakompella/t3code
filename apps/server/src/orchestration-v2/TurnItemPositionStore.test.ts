import { assert, it } from "@effect/vitest";
import { CommandId, RunId, ThreadId, TurnItemId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";

const layerTest = TurnItemPositionStore.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

it.effect("reuses an item's position without attempting another allocation insert", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const positions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const input = {
      threadId: ThreadId.make("thread:position-reuse"),
      turnItemId: TurnItemId.make("item:streaming"),
      runId: null,
    };
    const first = yield* positions.allocate(input);
    assert.equal(first, 1);

    // BEFORE INSERT runs even when ON CONFLICT would discard the allocation.
    yield* sql`
      CREATE TEMP TRIGGER reject_redundant_position_insert
      BEFORE INSERT ON orchestration_v2_turn_item_positions
      WHEN EXISTS (
        SELECT 1 FROM orchestration_v2_turn_item_positions
        WHERE thread_id = NEW.thread_id AND turn_item_id = NEW.turn_item_id
      )
      BEGIN
        SELECT RAISE(ABORT, 'Existing item positions must be reused');
      END
    `;

    const next = yield* positions.allocate({
      ...input,
      turnItemId: TurnItemId.make("item:next"),
    });
    assert.equal(next, first + 1);
    assert.equal(yield* positions.allocate(input), first);
    assert.equal(
      yield* positions.allocate({
        ...input,
        runId: RunId.make("run:moved"),
        runOrdinal: 2,
      }),
      first,
    );
    assert.equal(
      yield* positions.allocate({ ...input, threadId: ThreadId.make("thread:other") }),
      1,
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect("places a rollback summary after the run the conversation was rewound to", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const positions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const threadId = ThreadId.make("thread:rollback-summary-position");
    yield* sql`
      INSERT INTO orchestration_v2_projection_runs
        (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
      VALUES ('run:kept', ${threadId}, 3, 'primeAgent', 'completed', '2026-10-18T00:00:00.000Z', '{}')
    `;
    const summary = (id: string, rewoundToRunId: RunId | null) =>
      ({
        id: TurnItemId.make(id),
        threadId,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 0,
        status: "completed",
        title: null,
        startedAt: null,
        completedAt: null,
        updatedAt: DateTime.makeUnsafe("2026-10-18T00:00:00.000Z"),
        type: "rollback_summary",
        rollbackRequestId: CommandId.make(`rollback:${id}`),
        rewoundToRunId,
      }) as const;
    const afterKeptRun = yield* positions.normalize(
      summary("item:after-run-3", RunId.make("run:kept")),
    );
    const atThreadStart = yield* positions.normalize(summary("item:at-start", null));
    assert.equal(afterKeptRun.ordinal, 3_000_001);
    assert.equal(atThreadStart.ordinal, 1);
  }).pipe(Effect.provide(layerTest)),
);
