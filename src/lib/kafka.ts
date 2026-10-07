import { Kafka, logLevel, Producer, Consumer, EachMessagePayload } from 'kafkajs';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

// The Kafka client is shared across producers and consumers.
// It holds connection state, metadata cache, etc.
export const kafka = new Kafka({
  clientId: 'iot-marketplace',
  brokers,
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 300,
    retries: 8,
  },
});

// ─────────────────────────────────────────
// Producer
// ─────────────────────────────────────────
// A producer writes messages to topics. We use a single producer instance
// for the entire app (it's thread-safe and connection-pooled internally).
// Creating a new producer per request is a common beginner mistake — it
// kills performance.

let producerInstance: Producer | null = null;

export async function getProducer(): Promise<Producer> {
  if (producerInstance) return producerInstance;

  const producer = kafka.producer({
    // idempotent: true means Kafka deduplicates retries. If the producer
    // sends a message, doesn't get an ack, and retries, the broker won't
    // write it twice. This gives us "exactly-once" semantics per partition.
    idempotent: true,
    maxInFlightRequests: 5,
    allowAutoTopicCreation: false, // We create topics explicitly.
  });

  await producer.connect();
  producerInstance = producer;
  return producer;
}

export async function disconnectProducer(): Promise<void> {
  if (producerInstance) {
    await producerInstance.disconnect();
    producerInstance = null;
  }
}

// ─────────────────────────────────────────
// Consumer factory
// ─────────────────────────────────────────
// Each logical processor (storage-writer, alerting, analytics) gets its
// own consumer with its own groupId. They read the same topic
// independently.

export interface ConsumerOptions {
  groupId: string;
  topics: string[];
  handler: (payload: EachMessagePayload) => Promise<void>;
}

export async function startConsumer(options: ConsumerOptions): Promise<Consumer> {
  const consumer = kafka.consumer({
    groupId: options.groupId,
    // sessionTimeout: how long a consumer can be silent before Kafka
    // considers it dead and rebalances its partitions.
    sessionTimeout: 30000,
    // heartbeatInterval: how often the consumer pings Kafka to say
    // "I'm alive." Must be less than sessionTimeout/3.
    heartbeatInterval: 3000,
  });

  await consumer.connect();
  await consumer.subscribe({ topics: options.topics, fromBeginning: false });

  await consumer.run({
    // eachMessage is called once per message.
    // For higher throughput, use eachBatch — we'll switch to that on Day 6.
    eachMessage: async (payload) => {
      try {
        await options.handler(payload);
      } catch (err) {
        // Log but don't crash. A bad message shouldn't kill the consumer.
        // On Day 7 we'll add a proper dead-letter queue.
        console.error('Consumer handler failed:', err);
      }
    },
  });

  return consumer;
}