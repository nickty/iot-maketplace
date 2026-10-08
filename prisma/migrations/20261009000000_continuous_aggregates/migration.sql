-- ============================================================
-- Continuous aggregates for fast queries
-- ============================================================

-- 1-minute rollup: avg, min, max, count per (tenant, device, metric).
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 minute', timestamp) AS bucket,
  tenant_id,
  device_id,
  metric,
  AVG(value)::double precision AS avg_value,
  MIN(value)::double precision AS min_value,
  MAX(value)::double precision AS max_value,
  COUNT(*)::bigint AS sample_count
FROM telemetry
GROUP BY bucket, tenant_id, device_id, metric
WITH NO DATA;

-- Refresh policy: keep 1-minute buckets up to date.
-- start_offset = 5 minutes ago (how far back to look for changes)
-- end_offset = 1 minute ago (leave the current minute alone)
SELECT add_continuous_aggregate_policy('telemetry_1m',
  start_offset => INTERVAL '3 hours',
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute'
);

-- 1-hour rollup derived from the 1-minute rollup.
-- Bucketing from the smaller aggregate is faster and less data to scan.
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1h
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', bucket) AS bucket,
  tenant_id,
  device_id,
  metric,
  AVG(avg_value)::double precision AS avg_value,
  MIN(min_value)::double precision AS min_value,
  MAX(max_value)::double precision AS max_value,
  SUM(sample_count)::bigint AS sample_count
FROM telemetry_1m
GROUP BY time_bucket('1 hour', bucket), tenant_id, device_id, metric
WITH NO DATA;

SELECT add_continuous_aggregate_policy('telemetry_1h',
  start_offset => INTERVAL '30 days',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '10 minutes'
);

-- Indexes on the aggregates for fast lookup.
CREATE INDEX IF NOT EXISTS telemetry_1m_tenant_device_bucket_idx
  ON telemetry_1m (tenant_id, device_id, bucket DESC);

CREATE INDEX IF NOT EXISTS telemetry_1h_tenant_device_bucket_idx
  ON telemetry_1h (tenant_id, device_id, bucket DESC);