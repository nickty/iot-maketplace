import path from "path";
import fs from "fs/promises";
import parquet from "parquetjs-lite";

/**
 * Gold records are pre-aggregated. One row per
 * (tenant, device, metric, hour) — the "business question" shape.
 */
export interface GoldRecord {
  tenantId: string;
  deviceId: string;
  deviceName: string;
  metric: string;
  hour: string;          // ISO hour bucket
  avgValue: number;
  minValue: number;
  maxValue: number;
  sampleCount: number;
}

/**
 * Aggregate Silver records into Gold. Groups by (tenant, device, metric, hour).
 */
export function aggregateToGold(rows: any[]): GoldRecord[] {
  const buckets = new Map<string, {
    tenantId: string;
    deviceId: string;
    deviceName: string;
    metric: string;
    hour: string;
    values: number[];
  }>();

  for (const row of rows) {
    const ts = new Date(row.timestamp);
    // Truncate to the top of the hour, UTC.
    const hourBucket = new Date(Date.UTC(
      ts.getUTCFullYear(),
      ts.getUTCMonth(),
      ts.getUTCDate(),
      ts.getUTCHours()
    )).toISOString();

    const key = `${row.tenantId}|${row.deviceId}|${row.metric}|${hourBucket}`;

    let bucket = buckets.get(key);
    if (!bucket) {
      bucket = {
        tenantId: row.tenantId,
        deviceId: row.deviceId,
        deviceName: row.deviceName,
        metric: row.metric,
        hour: hourBucket,
        values: [],
      };
      buckets.set(key, bucket);
    }
    bucket.values.push(row.value);
  }

  const gold: GoldRecord[] = [];
  for (const bucket of buckets.values()) {
    gold.push({
      tenantId: bucket.tenantId,
      deviceId: bucket.deviceId,
      deviceName: bucket.deviceName,
      metric: bucket.metric,
      hour: bucket.hour,
      avgValue: bucket.values.reduce((a, b) => a + b, 0) / bucket.values.length,
      minValue: Math.min(...bucket.values),
      maxValue: Math.max(...bucket.values),
      sampleCount: bucket.values.length,
    });
  }

  return gold;
}

/**
 * Write Gold records to a Parquet file.
 */
export async function writeGoldFile(filePath: string, records: GoldRecord[]): Promise<void> {
  const schema = new parquet.ParquetSchema({
    tenantId:    { type: "UTF8" },
    deviceId:    { type: "UTF8" },
    deviceName:  { type: "UTF8" },
    metric:      { type: "UTF8" },
    hour:        { type: "TIMESTAMP_MILLIS" },
    avgValue:    { type: "DOUBLE" },
    minValue:    { type: "DOUBLE" },
    maxValue:    { type: "DOUBLE" },
    sampleCount: { type: "INT64" },
  });

  await fs.mkdir(path.dirname(filePath), { recursive: true });

  const writer = await parquet.ParquetWriter.openFile(schema, filePath, {
    compression: "SNAPPY",
  });

  for (const record of records) {
    await writer.appendRow({
      ...record,
      hour: new Date(record.hour).getTime(),
    });
  }

  await writer.close();
}