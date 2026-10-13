import { startConsumer } from "../lib/kafka";
import { ensureTopic } from "../lib/topics";
import { recordsToParquetBuffer, BronzeRecord } from "../lib/parquet";
import { writeToLake, getLakeRoot } from "../lib/lake-storage";

const CONSUMER_GROUP = "lake-writer";
const BATCH_MAX_RECORDS = 500;
const BATCH_MAX_WAIT_MS = 30_000;

/**
 * Consumer that writes telemetry to the Bronze layer of the data lake.
 *
 * Buffers records in memory and flushes when:
 *   - The buffer reaches BATCH_MAX_RECORDS, OR
 *   - BATCH_MAX_WAIT_MS elapses since the first record was buffered.
 *
 * This avoids the "small files problem" — one file per message would
 * create millions of tiny objects, slowing down every future query.
 */
export async function startLakeConsumer(topics: string[]): Promise<void> {
  for (const topic of topics) {
    await ensureTopic(topic);
  }

  // Per-topic buffer. A single tenant's traffic won't flush another's.
  const buffers = new Map<string, BronzeRecord[]>();
  const flushTimers = new Map<string, NodeJS.Timeout>();

  const scheduleFlush = (topic: string) => {
    if (flushTimers.has(topic)) return;
    const timer = setTimeout(() => {
      flushTimers.delete(topic);
      void flushTopic(topic);
    }, BATCH_MAX_WAIT_MS);
    flushTimers.set(topic, timer);
  };

  const flushTopic = async (topic: string) => {
    const records = buffers.get(topic);
    if (!records || records.length === 0) return;
    buffers.set(topic, []);

    const timer = flushTimers.get(topic);
    if (timer) {
      clearTimeout(timer);
      flushTimers.delete(topic);
    }

    try {
      await writeBatchToLake(topic, records);
    } catch (err) {
      console.error(`[lake-writer] flush failed for ${topic}:`, err);
      // Put the records back so the next flush attempt retries them.
      const existing = buffers.get(topic) ?? [];
      buffers.set(topic, [...records, ...existing]);
    }
  };

  await startConsumer({
    groupId: CONSUMER_GROUP,
    topics,
    handler: async ({ topic, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let envelope: any;
      try {
        envelope = JSON.parse(raw);
      } catch {
        console.warn("[lake-writer] invalid JSON, skipping");
        return;
      }

      const record: BronzeRecord = {
        tenantId: envelope.tenantId,
        deviceId: envelope.deviceId,
        metric: envelope.metric,
        value: envelope.value,
        unit: envelope.unit ?? null,
        timestamp: envelope.timestamp,
        receivedAt: envelope.receivedAt,
        latitude: envelope.location?.lat ?? null,
        longitude: envelope.location?.lng ?? null,
        metadata: envelope.metadata ?? {},
      };

      const buffer = buffers.get(topic) ?? [];
      buffer.push(record);
      buffers.set(topic, buffer);

      if (buffer.length >= BATCH_MAX_RECORDS) {
        await flushTopic(topic);
      } else {
        scheduleFlush(topic);
      }
    },
  });

  console.log(`[lake-writer] started, listening on: ${topics.join(", ")}`);
  console.log(`[lake-writer] writing to ${getLakeRoot()}`);
}

/**
 * Write a batch of records to the Bronze layer.
 *
 * Partitioning: tenant / year / month / day / hour
 *   - Per-tenant: compliance + isolation + easy deletes
 *   - Per-hour: most queries are time-bounded; this enables partition pruning
 */
async function writeBatchToLake(
  _topic: string,
  records: BronzeRecord[]
): Promise<void> {
  if (records.length === 0) return;

  // All records in a topic belong to one tenant (per-tenant topics).
  const tenantId = records[0].tenantId;

  // Use the earliest timestamp in the batch to pick the partition.
  // Cross-hour batches are rare with 30s flushes; when they happen,
  // the whole batch lands in the earliest hour. Silver will re-partition.
  const earliest = new Date(
    Math.min(...records.map((r) => new Date(r.timestamp).getTime()))
  );

  const yyyy = earliest.getUTCFullYear();
  const mm = String(earliest.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(earliest.getUTCDate()).padStart(2, "0");
  const hh = String(earliest.getUTCHours()).padStart(2, "0");

  const filename = `telemetry-${Date.now()}-${Math.random()
    .toString(36)
    .slice(2, 8)}.parquet`;

  const relativeKey = [
    "bronze",
    `tenant=${tenantId}`,
    `year=${yyyy}`,
    `month=${mm}`,
    `day=${dd}`,
    `hour=${hh}`,
    filename,
  ].join("/");

  const buffer = await recordsToParquetBuffer(records);
  const result = await writeToLake(relativeKey, buffer);

  console.log(
    `[lake-writer] wrote ${result.location} (${result.bytes} bytes, ${records.length} records)`
  );
}