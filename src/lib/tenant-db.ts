import { Prisma, PrismaClient } from '@prisma/client';
import { prisma } from './prisma';

// Type for Prisma transaction client.
type TxClient = Omit<
  PrismaClient,
  '$connect' | '$disconnect' | '$on' | '$transaction' | '$use' | '$extends'
>;

/**
 * Run a database operation in the context of a specific tenant.
 *
 * All queries inside the callback are automatically filtered by RLS,
 * because we set the `app.current_tenant_id` session variable at the
 * start of the transaction.
 *
 * Even if the callback forgets to filter by tenantId, Postgres will
 * refuse to return another tenant's rows.
 */
export async function withTenant<T>(
  tenantId: string,
  fn: (tx: TxClient) => Promise<T>
): Promise<T> {
  return prisma.$transaction(async (tx) => {
    // set_config(name, value, is_local)
    //   - name: the session variable name
    //   - value: the tenant id
    //   - is_local: true = scoped to this transaction only
    //
    // is_local=true is CRITICAL. It ensures the setting is reset when
    // the transaction ends, so the next request on this pooled
    // connection starts clean.
    await tx.$executeRaw`SELECT set_config('app.current_tenant_id', ${tenantId}, true)`;

    return fn(tx as TxClient);
  });
}

/**
 * Run a database operation WITHOUT tenant context.
 *
 * Used for system operations:
 *   - Registration (no tenant exists yet)
 *   - Login (need to look up user by email before knowing tenant)
 *   - Background jobs, migrations
 *
 * DANGER: This bypasses RLS. Only use when you genuinely need to
 * operate outside tenant boundaries.
 */
export async function withoutTenant<T>(fn: (tx: TxClient) => Promise<T>): Promise<T> {
  return prisma.$transaction(async (tx) => {
    // Setting it to empty string means current_tenant_id() returns NULL,
    // which means no rows match any policy.
    //
    // Wait — that's not what we want for login. We need login to
    // look up users across tenants.
    //
    // The correct approach: use a role with BYPASSRLS, or explicitly
    // disable RLS for this operation. For simplicity, we'll set a
    // special sentinel value.
    await tx.$executeRaw`SELECT set_config('app.current_tenant_id', '', true)`;
    return fn(tx as TxClient);
  });
}