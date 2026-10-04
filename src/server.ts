import dotenv from "dotenv";
dotenv.config();

import Fastify from "fastify";
import cors from "@fastify/cors";
import jwt from "@fastify/jwt";


import prismaPlugin from "./plugins/prisma";
import authPlugin from "./plugins/auth";
import authRoutes from "./routes/auth";



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

  // Register routes.
  await app.register(authRoutes, { prefix: "/api/v1" });

  await app.listen({ port, host: "0.0.0.0" });
  app.log.info(`Server listening on port ${port}`);
}

startServer().catch((err) => {
  app.log.error(err);
  process.exit(1);
});
