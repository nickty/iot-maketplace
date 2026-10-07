import crypto from 'crypto';
import { prisma } from './prisma';

// Generate a secure, URL-safe API key for a device.
// 32 bytes = 256 bits of entropy. Practically impossible to guess.
export function generateDeviceKey(): string {
  return crypto.randomBytes(32).toString('base64url');
}

export async function createDevice(params: {
  tenantId: string;
  name: string;
}) {
  return prisma.device.create({
    data: {
      tenantId: params.tenantId,
      name: params.name,
      deviceKey: generateDeviceKey(),
    },
  });
}

// List devices for a specific tenant only.
// The tenantId MUST come from the verified JWT — never from the request.
export async function listDevicesForTenant(tenantId: string) {
  return prisma.device.findMany({
    where: { tenantId },
    orderBy: { createdAt: 'desc' },
  });
}

// Fetch a single device, but ONLY if it belongs to the tenant.
// This is the isolation pattern: always include tenantId in the WHERE clause.
export async function getDeviceForTenant(deviceId: string, tenantId: string) {
  return prisma.device.findFirst({
    where: { id: deviceId, tenantId },
  });
}

// Update a device, but only if it belongs to the tenant.
// Prisma's updateMany returns a count — we use that to detect unauthorized access.
export async function updateDeviceForTenant(
  deviceId: string,
  tenantId: string,
  data: { name?: string; status?: 'ACTIVE' | 'INACTIVE' | 'REVOKED' }
) {
  const result = await prisma.device.updateMany({
    where: { id: deviceId, tenantId },
    data,
  });
  return result.count;
}

// Delete a device, but only if it belongs to the tenant.
export async function deleteDeviceForTenant(deviceId: string, tenantId: string) {
  const result = await prisma.device.deleteMany({
    where: { id: deviceId, tenantId },
  });
  return result.count;
}