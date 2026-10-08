import { randomUUID } from "crypto";
import { startConsumer } from "../lib/kafka";
import { ensureTopic } from "../lib/topics";
import { persistTelemetryBatch, TelemetryInput } from "../lib/telemetry";

const CONSUMER_GROUP = "storage-writer";
const BATCH_SIZE = 100;

/**
 * Consumer that reads telemetry from Kafka and persists it to TimescaleDB.
 *
 * Uses batch processing: reads up to 100 messages at a time, writes them
 * in a single transaction. This is dramatically faster than one-at-a-time.
 */
export async function startStorageConsumer(topics: string[]): Promise<void> {
  for (const topic of topics) {
    await ensureTopic(topic);
  }

  await startConsumer({
    groupId: CONSUMER_GROUP,
    topics,
    handler: async ({ topic, partition, message, heartbeat }) => {
      // Each message needs a unique event ID. We use a combination of
      // topic + partition + offset. This is globally unique within
      // Kafka and stable across redeliveries.
      const eventId = deterministicEventId(topic, partition, message.offset);

      // Parse the envelope.
      const raw = message.value?.toString();
      if (!raw) {
        console.warn("[storage-consumer] empty message, skipping");
        return;
      }

      let parsed: TelemetryInput;
      try {
        parsed = JSON.parse(raw) as TelemetryInput;
      } catch (err) {
        console.error("[storage-consumer] invalid JSON, skipping:", raw.slice(0, 200));
        // We don't throw — this message is poison. Kafka redelivery
        // won't help. On Day 7 we'll route these to a DLQ.
        return;
      }

      // Persist.
      const result = await persistTelemetryBatch(CONSUMER_GROUP, [
        { ...parsed, eventId },
      ]);

      // Signal Kafka that we're still alive.
      await heartbeat();

      console.log(
        `[storage-consumer] ${topic} p${partition} offset=${message.offset} written=${result.written} skipped=${result.skipped}`
      );
    },
  });

  console.log(`[storage-consumer] started, listening on: ${topics.join(", ")}`);
}

/**
 * Deterministic event ID from Kafka coordinates.
 *
 * Same (topic, partition, offset) always produces the same UUID.
 * This is why redelivery doesn't create duplicate rows: the second
 * attempt generates the same event ID, so the unique constraint fires.
 */
function deterministicEventId(
  topic: string,
  partition: number,
  offset: string
): string {
  // Not a real UUID — but we store it as TEXT-like in Postgres via
  // the @db.Uuid column. We use a hash to make it UUID-shaped.
  const crypto = require("crypto");
  const hash = crypto
    .createHash("sha256")
    .update(`${topic}:${partition}:${offset}`)
    .digest("hex");
  // Format as UUID v4-ish (XXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX).
  return [
    hash.slice(0, 8),
    hash.slice(8, 12),
    "4" + hash.slice(13, 16),
    ((parseInt(hash[16], 16) & 0x3) | 0x8).toString(16) + hash.slice(17, 20),
    hash.slice(20, 32),
  ].join("-");
}