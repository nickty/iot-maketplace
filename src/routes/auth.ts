import { FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import { registerTenant, findUserByEmail, verifyPassword } from '../lib/auth';

const registerSchema = z.object({
  tenantName: z.string().min(2).max(100),
  tenantType: z.enum(['PRODUCER', 'CONSUMER']),
  email: z.string().email(),
  password: z.string().min(8).max(128),
});

const loginSchema = z.object({
  email: z.string().email(),
  password: z.string().min(1),
});

const authRoutes: FastifyPluginAsync = async (app) => {
  // ─────────────────────────────────────────
  // POST /auth/register
  // Creates a new tenant AND its owner user.
  // ─────────────────────────────────────────
  app.post('/auth/register', async (request, reply) => {
    const parsed = registerSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: 'ValidationError',
        details: parsed.error.flatten(),
      });
    }

    const { tenantName, tenantType, email, password } = parsed.data;

    // Check for existing user before attempting insert.
    const existing = await findUserByEmail(email);
    if (existing) {
      return reply.code(409).send({ error: 'EmailAlreadyExists' });
    }

    const { tenant, user } = await registerTenant({
      tenantName,
      tenantType,
      email,
      password,
    });

    // Issue a JWT immediately so the client is logged in after registering.
    const token = app.jwt.sign(
      { userId: user.id, tenantId: tenant.id, role: user.role },
      { expiresIn: '7d' }
    );

    return reply.code(201).send({
      token,
      tenant: { id: tenant.id, name: tenant.name, type: tenant.type },
      user: { id: user.id, email: user.email, role: user.role },
    });
  });

  // ─────────────────────────────────────────
  // POST /auth/login
  // Verifies credentials and returns a JWT.
  // ─────────────────────────────────────────
  app.post('/auth/login', async (request, reply) => {
    const parsed = loginSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: 'ValidationError',
        details: parsed.error.flatten(),
      });
    }

    const { email, password } = parsed.data;

    const user = await findUserByEmail(email);

    // SECURITY NOTE: We return the same error for "user not found" and
    // "wrong password". This prevents attackers from discovering which
    // emails are registered.
    if (!user) {
      return reply.code(401).send({ error: 'InvalidCredentials' });
    }

    const valid = await verifyPassword(password, user.passwordHash);
    if (!valid) {
      return reply.code(401).send({ error: 'InvalidCredentials' });
    }

    const token = app.jwt.sign(
      { userId: user.id, tenantId: user.tenantId, role: user.role },
      { expiresIn: '7d' }
    );

    return reply.send({
      token,
      tenant: { id: user.tenant.id, name: user.tenant.name, type: user.tenant.type },
      user: { id: user.id, email: user.email, role: user.role },
    });
  });

  // ─────────────────────────────────────────
  // GET /auth/me
  // Returns the current user (requires JWT).
  // This is a good test route to prove auth works.
  // ─────────────────────────────────────────
  app.get('/auth/me', { preHandler: [app.authenticate] }, async (request, reply) => {
    const user = await app.prisma.user.findUnique({
      where: { id: request.user.userId },
      include: { tenant: true },
    });

    if (!user) {
      return reply.code(404).send({ error: 'UserNotFound' });
    }

    return reply.send({
      user: { id: user.id, email: user.email, role: user.role },
      tenant: { id: user.tenant.id, name: user.tenant.name, type: user.tenant.type },
    });
  });
};

export default authRoutes;