import { startConsumer } from '../lib/kafka';
import { ensureTopic } from '../lib/topics';

/**
 * A simple consumer that logs every telemetry message.
 *
 * Its only job for Day 5 is to prove the pipeline works end to end:
 *   Device → API → Kafka → Consumer → stdout
 *
 * Tomorrow, we'll swap this out for a consumer that writes to TimescaleDB.
 */
export async function startLogConsumer(topics: string[]): Promise<void> {
  // Ensure topics exist before subscribing, otherwise the consumer
  // will wait forever for a topic that never appears.
  for (const topic of topics) {
    await ensureTopic(topic);
  }

  await startConsumer({
    groupId: 'log-consumer',
    topics,
    handler: async ({ topic, partition, message }) => {
      const key = message.key?.toString() ?? 'no-key';
      const value = message.value?.toString() ?? '';

      console.log(
        `[log-consumer] ${topic} p${partition} offset=${message.offset} key=${key} value=${value}`
      );
    },
  });

  console.log(`[log-consumer] started, listening on: ${topics.join(', ')}`);
}