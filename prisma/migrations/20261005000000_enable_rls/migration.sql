-- ============================================================
-- Enable Row-Level Security with two-role pattern
-- ============================================================

-- 1. Create two roles.
--    The app connects as `marketplace_app` for tenant-scoped ops.
--    Login/registration uses `marketplace_admin` (bypasses RLS).

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'marketplace_app') THEN
    CREATE ROLE marketplace_app NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'marketplace_admin') THEN
    CREATE ROLE marketplace_admin NOLOGIN BYPASSRLS;
  END IF;
END
$$;

-- 2. Grant table access.
GRANT SELECT, INSERT, UPDATE, DELETE ON tenants, users, devices TO marketplace_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON tenants, users, devices TO marketplace_admin;

-- 3. Grant sequence/function access if needed (UUID tables don't need this).

-- 4. Enable RLS.
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE users   ENABLE ROW LEVEL SECURITY;
ALTER TABLE devices ENABLE ROW LEVEL SECURITY;

ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
ALTER TABLE users   FORCE ROW LEVEL SECURITY;
ALTER TABLE devices FORCE ROW LEVEL SECURITY;

-- 5. Helper function.
CREATE OR REPLACE FUNCTION current_tenant_id() RETURNS uuid AS $$
BEGIN
  RETURN NULLIF(current_setting('app.current_tenant_id', true), '')::uuid;
END;
$$ LANGUAGE plpgsql STABLE;

-- 6. Policies — apply only to marketplace_app.
--    marketplace_admin has BYPASSRLS, so these don't apply to it.

DROP POLICY IF EXISTS tenants_isolation ON tenants;
CREATE POLICY tenants_isolation ON tenants
  FOR ALL TO marketplace_app
  USING (id = current_tenant_id())
  WITH CHECK (id = current_tenant_id());

DROP POLICY IF EXISTS users_isolation ON users;
CREATE POLICY users_isolation ON users
  FOR ALL TO marketplace_app
  USING (tenant_id = current_tenant_id())
  WITH CHECK (tenant_id = current_tenant_id());

DROP POLICY IF EXISTS devices_isolation ON devices;
CREATE POLICY devices_isolation ON devices
  FOR ALL TO marketplace_app
  USING (tenant_id = current_tenant_id())
  WITH CHECK (tenant_id = current_tenant_id());