import { kafka } from './kafka';

// Topic naming convention for multi-tenancy.
// Each tenant gets their own telemetry topic.
// This gives us:
//   - Logical isolation (easy to reason about)
//   - Per-tenant retention policies (delete after N days)
//   - Per-tenant access control (Kafka ACLs, added later)
//   - Easy cleanup (delete one topic, delete one tenant's data)
export function tenantTelemetryTopic(tenantId: string): string {
  return `tenant.${tenantId}.telemetry`;
}

// Alert topic per tenant — real-time rule matches.
export function tenantAlertTopic(tenantId: string): string {
  return `tenant.${tenantId}.alerts`;
}

// Create a topic if it doesn't already exist.
// Idempotent — safe to call multiple times.
export async function ensureTopic(
  topic: string,
  options: { partitions?: number; replicationFactor?: number } = {}
): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();

  try {
    const existing = await admin.listTopics();
    if (!existing.includes(topic)) {
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: options.partitions ?? 1,
            replicationFactor: options.replicationFactor ?? 1,
          },
        ],
        waitForLeaders: true,
      });
    }
  } finally {
    await admin.disconnect();
  }
}