import { PrismaClient } from '@prisma/client';

// Why a singleton? In development with hot reload, Node.js can create
// multiple PrismaClient instances, exhausting DB connections.
// This pattern ensures only one instance exists.
const globalForPrisma = globalThis as unknown as {
  prisma: PrismaClient | undefined;
};

export const prisma =
  globalForPrisma.prisma ??
  new PrismaClient({
    log: process.env.NODE_ENV === 'development' ? ['query', 'error', 'warn'] : ['error'],
  });

if (process.env.NODE_ENV !== 'production') {
  globalForPrisma.prisma = prisma;
}