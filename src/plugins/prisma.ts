import { FastifyPluginAsync } from 'fastify';
import fp from 'fastify-plugin';
import { prisma } from '../lib/prisma';

declare module 'fastify' {
  interface FastifyInstance {
    prisma: typeof prisma;
  }
}

const prismaPlugin: FastifyPluginAsync = async (app) => {
  app.decorate('prisma', prisma);
  app.addHook('onClose', async () => {
    await prisma.$disconnect();
  });
};

export default fp(prismaPlugin, { name: 'prisma' });