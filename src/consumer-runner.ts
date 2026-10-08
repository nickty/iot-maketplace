import dotenv from "dotenv";
dotenv.config();

import { ensureTopic, tenantTelemetryTopic } from "./lib/topics";
import { prismaAdmin } from "./lib/prisma";
import { startLogConsumer } from "./consumers/log-consumer";
import { startStorageConsumer } from "./consumers/storage-consumer";

async function startConsumers() {
  const allTenants = await prismaAdmin.tenant.findMany({ select: { id: true } });
  const telemetryTopics = allTenants.map((t) => tenantTelemetryTopic(t.id));

  for (const topic of telemetryTopics) {
    await ensureTopic(topic);
  }

  if (telemetryTopics.length === 0) {
    console.log("[consumer-runner] No tenants yet. Consumers will not start.");
    return;
  }

  // Start both consumers in parallel. They run in the same Node.js
  // process but with different consumer groups — independent bookmarks,
  // independent partitions, independent processing.
  await Promise.all([
    startLogConsumer(telemetryTopics),
    startStorageConsumer(telemetryTopics),
  ]);
}

startConsumers().catch((err) => {
  console.error("[consumer-runner] Fatal error:", err);
  process.exit(1);
});