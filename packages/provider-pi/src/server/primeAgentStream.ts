import * as Effect from "effect/Effect";
import {
  piRecordField as recordField,
  piRecordNumber as recordNumber,
  piRecordString as recordString,
} from "./rpc.ts";
import type { PiItemSink, PiStreamItemState } from "./adapterState.ts";

/** The streamable text a Pi message snapshot holds for one content block. */
export function snapshotBlock(
  message: unknown,
  contentIndex: number,
): { readonly kind: PiStreamItemState["kind"]; readonly text: string } | undefined {
  const content = recordField(message, "content");
  if (!Array.isArray(content)) return undefined;
  const block: unknown = content[contentIndex];
  const type = recordField(block, "type");
  const text =
    type === "text"
      ? recordString(block, "text")
      : type === "thinking"
        ? recordString(block, "thinking")
        : undefined;
  if (text === undefined || text.length === 0) return undefined;
  return { kind: type === "text" ? "assistant_message" : "reasoning", text };
}

export function makePrimeAgentStream(input: {
  readonly lossyStream: boolean;
  readonly streamItemFor: (
    turn: PiItemSink,
    kind: PiStreamItemState["kind"],
    contentIndex: number,
  ) => Effect.Effect<PiStreamItemState>;
  /** Reserves the place of the sink's current message's block, for an item made later. */
  readonly reserveStreamItem: (turn: PiItemSink, contentIndex: number) => void;
  /**
   * Gives the current message's block, if it has no place yet, the place
   * reserved for the same block of message `messageOrdinal`.
   */
  readonly inheritStreamItemPlace: (
    turn: PiItemSink,
    messageOrdinal: number,
    contentIndex: number,
  ) => void;
  readonly completeStreamItem: (
    turn: PiItemSink,
    item: PiStreamItemState,
    text?: string,
  ) => Effect.Effect<void>;
  readonly scheduleStreamFlush: (turn: PiItemSink, item: PiStreamItemState) => Effect.Effect<void>;
  /** Sends a completed item's current text. */
  readonly emitCompletedStreamItem: (
    turn: PiItemSink,
    item: PiStreamItemState,
  ) => Effect.Effect<void>;
}) {
  const {
    lossyStream,
    streamItemFor,
    reserveStreamItem,
    inheritStreamItemPlace,
    completeStreamItem,
    scheduleStreamFlush,
    emitCompletedStreamItem,
  } = input;
  /**
   * Lossy streams (see `PiFlavor.lossyStream`) can drop `message_start`, so
   * counting starts would give a later message the id of an earlier,
   * completed one. Their messages are keyed by the snapshot's own
   * timestamp instead, on every event. Returns whether the snapshot had one.
   */
  const adoptMessageIdentity = (turn: PiItemSink, message: unknown) => {
    if (!lossyStream) return false;
    const timestamp = recordNumber(message, "timestamp");
    if (timestamp === undefined) return false;
    turn.messageOrdinal = timestamp;
    return true;
  };

  /**
   * Take the text of every block in a message snapshot as the truth. The
   * lossy streams (see `PiFlavor.lossyStream`) skip deltas, block ends, and
   * whole blocks, but a snapshot is cumulative. A running snapshot keeps
   * its items open; the final message completes them.
   */
  const adoptSnapshot = Effect.fnUntraced(function* (
    turn: PiItemSink,
    message: unknown,
    final: boolean,
  ) {
    const content = recordField(message, "content");
    if (!Array.isArray(content)) return;
    for (let contentIndex = 0; contentIndex < content.length; contentIndex += 1) {
      const block = snapshotBlock(message, contentIndex);
      if (block === undefined) continue;
      const item = yield* streamItemFor(turn, block.kind, contentIndex);
      if (item.completed) continue;
      item.text = block.text;
      if (final) yield* completeStreamItem(turn, item);
      else yield* scheduleStreamFlush(turn, item);
    }
  });

  /**
   * Keeps the place a message arrived in, for items a history read makes
   * later, without showing anything.
   */
  const reserveMessage = (turn: PiItemSink, message: unknown) => {
    const liveMessageOrdinal = turn.messageOrdinal;
    if (!adoptMessageIdentity(turn, message)) return;
    const content = recordField(message, "content");
    (Array.isArray(content) ? content : []).forEach((_, contentIndex) => {
      if (snapshotBlock(message, contentIndex) !== undefined) reserveStreamItem(turn, contentIndex);
    });
    turn.messageOrdinal = liveMessageOrdinal;
  };

  /**
   * Brings every item of a stored assistant message to the text Prime Agent recorded, whether or
   * not the stream started it. Messages match by the timestamp identity from
   * `adoptMessageIdentity`, never by text. A stopped copy takes the identity of the reply it
   * `replaced`.
   */
  const upsertRecordedMessage = Effect.fnUntraced(function* (
    turn: PiItemSink,
    message: unknown,
    replaced?: number,
  ) {
    const liveMessageOrdinal = turn.messageOrdinal;
    if (!adoptMessageIdentity(turn, message)) return;
    const ownMessageOrdinal = turn.messageOrdinal;
    if (replaced !== undefined) turn.messageOrdinal = replaced;
    const content = recordField(message, "content");
    const blockCount = Array.isArray(content) ? content.length : 0;
    for (let contentIndex = 0; contentIndex < blockCount; contentIndex += 1) {
      const block = snapshotBlock(message, contentIndex);
      if (block === undefined) continue;
      if (replaced !== undefined) inheritStreamItemPlace(turn, ownMessageOrdinal, contentIndex);
      const item = yield* streamItemFor(turn, block.kind, contentIndex);
      if (item.completed && item.text === block.text) continue;
      item.text = block.text;
      item.completed = true;
      yield* emitCompletedStreamItem(turn, item);
    }
    turn.messageOrdinal = liveMessageOrdinal;
  });

  /**
   * Whether `copy` holds every block T3 shows of the reply stamped
   * `replaced`, as stock's copy of a cut reply does.
   */
  const holdsShownText = (turn: PiItemSink, copy: unknown, replaced: number) =>
    [...turn.streamItems.values()].every((item) => {
      if (item.messageOrdinal !== replaced) return true;
      const block = snapshotBlock(copy, item.contentIndex);
      return block?.kind === item.kind && block.text.startsWith(item.text);
    });

  return {
    adoptMessageIdentity,
    adoptSnapshot,
    reserveMessage,
    upsertRecordedMessage,
    holdsShownText,
  };
}
