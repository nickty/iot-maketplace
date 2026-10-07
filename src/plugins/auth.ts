import { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';

// This is the shape of the JWT payload we issue and verify.
export interface JwtPayload {
  userId: string;
  tenantId: string;
  role: 'OWNER' | 'ADMIN' | 'MEMBER';
}

// Correct TypeScript module augmentation for @fastify/jwt
declare module '@fastify/jwt' {
  interface FastifyJWT {
    payload: JwtPayload; // Payload type inside the token
    user: JwtPayload;    // Type of request.user
  }
}

// Extend FastifyInstance to recognize app.authenticate decorator
declare module 'fastify' {
  interface FastifyRequest {
    user: JwtPayload;
  }
  interface FastifyInstance {
    authenticate: (request: FastifyRequest, reply: FastifyReply) => Promise<void>;
  }
}

const authPlugin: FastifyPluginAsync = async (app) => {
  // A reusable "preHandler" hook. Attach this to any route that
  // requires a logged-in user.
  app.decorate('authenticate', async (request: FastifyRequest, reply: FastifyReply) => {
    try {
      // verifyJWT decodes the token, checks the signature, checks expiry,
      // and if all good, returns the payload.
      const payload = await request.jwtVerify<JwtPayload>();
      request.user = payload;
    } catch (err) {
      reply.code(401).send({ error: 'Unauthorized', message: 'Invalid or expired token' });
    }
  });
};

export default fp(authPlugin, {
  name: 'auth',
  dependencies: ['@fastify/jwt'],
});