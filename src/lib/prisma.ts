import { PrismaClient } from '@prisma/client';

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

// ─────────────────────────────────────────────────────────────
// Alias for now. When we add RLS (Day 4, deferred), this will
// become a separate PrismaClient with a different DATABASE_URL
// that bypasses RLS for login/registration.
// ─────────────────────────────────────────────────────────────
export const prismaAdmin = prisma;