import dotenv from "dotenv";
dotenv.config();

import { ensureTopic, tenantTelemetryTopic } from "./lib/topics";
import { prismaAdmin } from "./lib/prisma";
import { startLogConsumer } from "./consumers/log-consumer";

async function startConsumers() {
  // Discover existing tenants.
  const allTenants = await prismaAdmin.tenant.findMany({ select: { id: true } });
  const telemetryTopics = allTenants.map((t) => tenantTelemetryTopic(t.id));

  for (const topic of telemetryTopics) {
    await ensureTopic(topic);
  }

  if (telemetryTopics.length > 0) {
    await startLogConsumer(telemetryTopics);
  } else {
    console.log("[consumer-runner] No tenants yet. Consumer will not start.");
  }
}

startConsumers().catch((err) => {
  console.error("[consumer-runner] Fatal error:", err);
  process.exit(1);
});