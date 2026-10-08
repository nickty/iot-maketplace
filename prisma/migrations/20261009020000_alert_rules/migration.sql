CREATE TABLE IF NOT EXISTS alert_rules (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   UUID NOT NULL,
  device_id   UUID,
  metric      TEXT NOT NULL,
  operator    TEXT NOT NULL,
  threshold   DOUBLE PRECISION NOT NULL,
  severity    TEXT NOT NULL,
  enabled     BOOLEAN NOT NULL DEFAULT true,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alert_rules_tenant_enabled_idx
  ON alert_rules (tenant_id, enabled);