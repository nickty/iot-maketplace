-- CreateTable
CREATE TABLE "telemetry" (
    "id" UUID NOT NULL,
    "tenant_id" UUID NOT NULL,
    "device_id" UUID NOT NULL,
    "metric" TEXT NOT NULL,
    "value" DOUBLE PRECISION NOT NULL,
    "unit" TEXT,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "received_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "latitude" DOUBLE PRECISION,
    "longitude" DOUBLE PRECISION,
    "metadata" JSONB NOT NULL DEFAULT '{}',

    CONSTRAINT "telemetry_pkey" PRIMARY KEY ("id","timestamp")
);

-- CreateTable
CREATE TABLE "processed_events" (
    "event_id" UUID NOT NULL,
    "consumer" TEXT NOT NULL,
    "processed_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_events_pkey" PRIMARY KEY ("event_id")
);

-- CreateIndex
CREATE INDEX "telemetry_tenant_id_timestamp_idx" ON "telemetry"("tenant_id", "timestamp" DESC);

-- CreateIndex
CREATE INDEX "telemetry_device_id_timestamp_idx" ON "telemetry"("device_id", "timestamp" DESC);

-- CreateIndex
CREATE INDEX "processed_events_processed_at_idx" ON "processed_events"("processed_at" DESC);
