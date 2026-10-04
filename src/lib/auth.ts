import bcrypt from 'bcrypt';
import { prisma } from './prisma';

const SALT_ROUNDS = 12;

export async function hashPassword(password: string): Promise<string> {
  return bcrypt.hash(password, SALT_ROUNDS);
}

export async function verifyPassword(password: string, hash: string): Promise<boolean> {
  return bcrypt.compare(password, hash);
}

// Register a new tenant + its owner user in a single transaction.
// If either step fails, neither is created — this is atomicity.
export async function registerTenant(params: {
  tenantName: string;
  tenantType: 'PRODUCER' | 'CONSUMER';
  email: string;
  password: string;
}) {
  const passwordHash = await hashPassword(params.password);

  // Prisma transaction: both operations succeed or both roll back.
  const result = await prisma.$transaction(async (tx) => {
    const tenant = await tx.tenant.create({
      data: {
        name: params.tenantName,
        type: params.tenantType,
      },
    });

    const user = await tx.user.create({
      data: {
        tenantId: tenant.id,
        email: params.email,
        passwordHash,
        role: 'OWNER',
      },
    });

    return { tenant, user };
  });

  return result;
}

// Find a user by email, along with their tenant.
export async function findUserByEmail(email: string) {
  return prisma.user.findUnique({
    where: { email },
    include: { tenant: true },
  });
}