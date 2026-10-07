import { FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import {
  createDevice,
  listDevicesForTenant,
  getDeviceForTenant,
  updateDeviceForTenant,
  deleteDeviceForTenant,
} from '../lib/devices';

const createDeviceSchema = z.object({
  name: z.string().min(1).max(100),
});

const updateDeviceSchema = z.object({
  name: z.string().min(1).max(100).optional(),
  status: z.enum(['ACTIVE', 'INACTIVE', 'REVOKED']).optional(),
});

const deviceRoutes: FastifyPluginAsync = async (app) => {
  // All routes here require authentication.
  // We register the hook once at the plugin level instead of per-route.
  app.addHook('preHandler', app.authenticate);

  // ─────────────────────────────────────────
  // POST /devices
  // Create a device owned by the calling tenant.
  // ─────────────────────────────────────────
  app.post('/devices', async (request, reply) => {
    const parsed = createDeviceSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: 'ValidationError',
        details: parsed.error.flatten(),
      });
    }

    const device = await createDevice({
      tenantId: request.user.tenantId,
      name: parsed.data.name,
    });

    return reply.code(201).send({ device });
  });

  // ─────────────────────────────────────────
  // GET /devices
  // List devices for the calling tenant ONLY.
  // ─────────────────────────────────────────
  app.get('/devices', async (request, reply) => {
    const devices = await listDevicesForTenant(request.user.tenantId);
    return reply.send({ devices });
  });

  // ─────────────────────────────────────────
  // GET /devices/:id
  // Fetch one device — returns 404 if it belongs to another tenant.
  // ─────────────────────────────────────────
  app.get<{ Params: { id: string } }>('/devices/:id', async (request, reply) => {
    const device = await getDeviceForTenant(request.params.id, request.user.tenantId);

    if (!device) {
      // 404 for both "doesn't exist" and "belongs to someone else".
      // This prevents attackers from probing which device IDs exist.
      return reply.code(404).send({ error: 'DeviceNotFound' });
    }

    return reply.send({ device });
  });

  // ─────────────────────────────────────────
  // PATCH /devices/:id
  // Update a device — only if it belongs to the calling tenant.
  // ─────────────────────────────────────────
  app.patch<{ Params: { id: string } }>('/devices/:id', async (request, reply) => {
    const parsed = updateDeviceSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: 'ValidationError',
        details: parsed.error.flatten(),
      });
    }

    // Only OWNER and ADMIN can modify devices. MEMBERs are read-only.
    if (request.user.role === 'MEMBER') {
      return reply.code(403).send({ error: 'Forbidden' });
    }

    const count = await updateDeviceForTenant(
      request.params.id,
      request.user.tenantId,
      parsed.data
    );

    if (count === 0) {
      return reply.code(404).send({ error: 'DeviceNotFound' });
    }

    const device = await getDeviceForTenant(request.params.id, request.user.tenantId);
    return reply.send({ device });
  });

  // ─────────────────────────────────────────
  // DELETE /devices/:id
  // Delete a device — only if it belongs to the calling tenant.
  // ─────────────────────────────────────────
  app.delete<{ Params: { id: string } }>('/devices/:id', async (request, reply) => {
    // Only OWNER and ADMIN can delete devices.
    if (request.user.role === 'MEMBER') {
      return reply.code(403).send({ error: 'Forbidden' });
    }

    const count = await deleteDeviceForTenant(request.params.id, request.user.tenantId);

    if (count === 0) {
      return reply.code(404).send({ error: 'DeviceNotFound' });
    }

    return reply.code(204).send();
  });
};

export default deviceRoutes;