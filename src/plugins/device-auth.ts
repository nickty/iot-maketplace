import { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { prisma } from '../lib/prisma';

export interface AuthenticatedDevice {
  id: string;
  tenantId: string;
  name: string;
}

declare module 'fastify' {
  interface FastifyRequest {
    device?: AuthenticatedDevice;
  }

  // ← ADD THIS BLOCK
  interface FastifyInstance {
    authenticateDevice: (request: FastifyRequest, reply: FastifyReply) => Promise<void>;
  }
}

const deviceAuthPlugin: FastifyPluginAsync = async (app) => {
  app.decorate('authenticateDevice', async (request: FastifyRequest, reply: FastifyReply) => {
    const key = request.headers['x-device-key'];

    if (!key || typeof key !== 'string') {
      return reply.code(401).send({ error: 'MissingDeviceKey' });
    }

    const device = await prisma.device.findFirst({
      where: { deviceKey: key, status: 'ACTIVE' },
      select: { id: true, tenantId: true, name: true },
    });

    if (!device) {
      return reply.code(401).send({ error: 'InvalidDeviceKey' });
    }

    request.device = device;
  });
};

export default fp(deviceAuthPlugin, { name: 'device-auth' });