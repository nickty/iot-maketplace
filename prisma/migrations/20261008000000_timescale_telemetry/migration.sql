-- ============================================================
-- TimescaleDB extension + telemetry hypertable
-- ============================================================

-- 1. Enable TimescaleDB. Requires superuser on first run — the
--    `marketplace` user is a superuser because Docker created it
--    with POSTGRES_USER=marketplace.
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 2. Create the telemetry table.
CREATE TABLE IF NOT EXISTS telemetry (
  id           UUID NOT NULL DEFAULT gen_random_uuid(),
  tenant_id    UUID NOT NULL,
  device_id    UUID NOT NULL,
  metric       TEXT NOT NULL,
  value        DOUBLE PRECISION NOT NULL,
  unit         TEXT,
  timestamp    TIMESTAMPTZ NOT NULL,
  received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  latitude     DOUBLE PRECISION,
  longitude    DOUBLE PRECISION,
  metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- Composite primary key: id + timestamp.
  -- TimescaleDB requires the partition column (timestamp) in every
  -- unique constraint. This is because uniqueness is enforced per
  -- chunk, not globally.
  PRIMARY KEY (id, timestamp)
);

-- 3. Convert to a hypertable.
--    'timestamp' is the partition column.
--    chunk_time_interval = 1 day: one physical table per day.
--    If partitioning by time fails because the table is non-empty,
--    migrate_data => TRUE handles it (but ours is empty).
SELECT create_hypertable(
  'telemetry',
  'timestamp',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE
);

-- 4. Indexes for common query patterns.
--    (timestamp DESC) is the primary access path: "latest N for tenant".
CREATE INDEX IF NOT EXISTS telemetry_tenant_timestamp_idx
  ON telemetry (tenant_id, timestamp DESC);

--    Per-device time queries: "all readings from device X".
CREATE INDEX IF NOT EXISTS telemetry_device_timestamp_idx
  ON telemetry (device_id, timestamp DESC);

--    Metric filtering across a tenant.
CREATE INDEX IF NOT EXISTS telemetry_tenant_metric_timestamp_idx
  ON telemetry (tenant_id, metric, timestamp DESC);

-- 5. Enable compression on chunks older than 7 days.
--    This is a two-step process: first declare the settings,
--    then set the policy.
ALTER TABLE telemetry SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'tenant_id, device_id',
  timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('telemetry', INTERVAL '7 days');

-- 6. Table for idempotent consumer tracking.
--    Every message we process is recorded here by its unique ID.
--    If we see the same ID again, we skip the insert.
CREATE TABLE IF NOT EXISTS processed_events (
  event_id    UUID PRIMARY KEY,
  consumer    TEXT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Auto-cleanup: keep the dedup log for 30 days. Older than that,
-- Kafka's retention would have expired anyway, so no duplicate
-- could arrive.
CREATE INDEX IF NOT EXISTS processed_events_processed_at_idx
  ON processed_events (processed_at DESC);