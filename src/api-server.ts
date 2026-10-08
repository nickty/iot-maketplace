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

const app = Fastify({
  logger: { level: process.env.NODE_ENV === "development" ? "info" : "warn" },
});

app.get("/health", async () => ({
  status: "ok",
  timestamp: new Date().toISOString(),
}));

const port = Number(process.env.PORT) || 3000;

async function startApiServer() {
  await app.register(cors, { origin: true });
  await app.register(jwt, { secret: process.env.JWT_SECRET! });
  await app.register(prismaPlugin);
  await app.register(authPlugin);
  await app.register(deviceAuthPlugin);

  await app.register(authRoutes, { prefix: "/api/v1" });
  await app.register(deviceRoutes, { prefix: "/api/v1" });
  await app.register(ingestRoutes, { prefix: "/api/v1" });

  await app.listen({ port, host: "0.0.0.0" });
  app.log.info(`API server listening on port ${port}`);
}

startApiServer().catch((err) => {
  app.log.error(err);
  process.exit(1);
});