import dotenv from "dotenv";
dotenv.config();

import Fastify from "fastify";
import cors from "@fastify/cors";
import jwt from "@fastify/jwt";
import deviceAuthPlugin from "./plugins/device-auth";
import ingestRoutes from "./routes/ingest";

import prismaPlugin from "./plugins/prisma";
import authPlugin from "./plugins/auth";
import authRoutes from "./routes/auth";
import deviceRoutes from "./routes/devices";
import { ensureTopic, tenantTelemetryTopic } from "./lib/topics";
import { prismaAdmin } from "./lib/prisma";
import { startLogConsumer } from "./consumers/log-consumer";

const app = Fastify({
  logger: {
    level: process.env.NODE_ENV === "development" ? "info" : "warn",
  },
});

// Health check — every service should have one.
// Kubernetes will use this later to know if the app is alive.
app.get("/health", async () => ({
  status: "ok",
  timestamp: new Date().toISOString(),
}));

// Start the server
const port = Number(process.env.PORT) || 3000;

async function startServer() {
  // Register CORS — allows browsers from other domains to call your API.
  // In production, restrict origin to your actual frontend domain.
  await app.register(cors, { origin: true });
  await app.register(jwt, { secret: process.env.JWT_SECRET! });

  // Order matters: prisma first (no deps), then auth (needs jwt).
  await app.register(prismaPlugin);
  await app.register(authPlugin);
  await app.register(deviceAuthPlugin);

  // Register routes.
  await app.register(authRoutes, { prefix: "/api/v1" });
  await app.register(deviceRoutes, { prefix: "/api/v1" });
  await app.register(ingestRoutes, { prefix: "/api/v1" });

  // Before starting consumers, discover all existing tenants and ensure
  // their telemetry topics exist. Consumers subscribe to a fixed list of
  // topics — they can't "watch for new tenants" natively.
  //
  // For Day 5, this is fine. Later, we'll have a separate "topic manager"
  // that reacts to tenant creation events.
  const allTenants = await prismaAdmin.tenant.findMany({
    select: { id: true },
  });
  const telemetryTopics = allTenants.map((t) => tenantTelemetryTopic(t.id));

  // Ensure they all exist.
  for (const topic of telemetryTopics) {
    await ensureTopic(topic);
  }

  // Start the log consumer, but only if there's at least one topic.
  // KafkaJS fails if you subscribe to zero topics.
  if (telemetryTopics.length > 0) {
    await startLogConsumer(telemetryTopics);
  } else {
    app.log.info("No tenants yet. Log consumer will not start.");
  }

  await app.listen({ port, host: "0.0.0.0" });
  app.log.info(`Server listening on port ${port}`);
}

startServer().catch((err) => {
  app.log.error(err);
  process.exit(1);
});
