import path from "path";
import fs from "fs/promises";
import parquet from "parquetjs-lite";
import { prisma } from "./prisma";
import { exec } from "child_process";
import { promisify } from "util";
import { Database } from "duckdb-async";
const execAsync = promisify(exec);

/**
 * A single Silver record. This is the "cleaned and enriched" shape.
 * Every field a downstream consumer needs is here, denormalized.
 */
export interface SilverRecord {
  eventId: string;
  tenantId: string;
  deviceId: string;
  deviceName: string; // enriched from devices table
  metric: string;
  value: number;
  unit: string | null;
  timestamp: string;
  receivedAt: string;
  latitude: number | null;
  longitude: number | null;
  metadata: string; // JSON string
}

/**
 * Read every row from a Bronze Parquet file.
 */
export async function readBronzeFile(filePath: string): Promise<any[]> {
  const db = await Database.create(":memory:");
  const normalizedPath = filePath.replace(/\\/g, "/");
  const rows = await db.all(
    `SELECT * FROM read_parquet('${normalizedPath}')`
  );
  await db.close();
  return rows;
}

/**
 * Deduplicate records by (tenantId, deviceId, metric, timestamp).
 * This is a logical dedupe — two records with identical values are
 * treated as duplicates even if their Kafka offsets differ.
 *
 * The real platform should use a stronger key (like the Kafka eventId),
 * but Bronze doesn't store that today. This is a pragmatic stand-in.
 */
export function dedupe(rows: any[]): any[] {
  const seen = new Set<string>();
  const unique: any[] = [];

  for (const row of rows) {
    const key = `${row.tenantId}|${row.deviceId}|${row.metric}|${row.timestamp}`;
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(row);
  }

  return unique;
}

/**
 * Enrich rows with device metadata from Postgres.
 * Batches device lookups by tenant to avoid N queries.
 */
export async function enrich(
  rows: any[],
  deviceCache: Map<string, { name: string }>,
): Promise<SilverRecord[]> {
  const enriched: SilverRecord[] = [];

  for (const row of rows) {
    // Cache miss — look up the device and remember the result.
    let device = deviceCache.get(row.deviceId);
    if (!device) {
      const dbDevice = await prisma.device.findUnique({
        where: { id: row.deviceId },
        select: { name: true },
      });
      device = { name: dbDevice?.name ?? "unknown" };
      deviceCache.set(row.deviceId, device);
    }

    enriched.push({
      eventId: `${row.deviceId}-${row.metric}-${row.timestamp}`,
      tenantId: row.tenantId,
      deviceId: row.deviceId,
      deviceName: device.name,
      metric: row.metric,
      value: row.value,
      unit: row.unit ?? null,
      timestamp: new Date(row.timestamp).toISOString(),
      receivedAt: new Date(row.receivedAt).toISOString(),
      latitude: row.latitude ?? null,
      longitude: row.longitude ?? null,
      metadata:
        typeof row.metadata === "string"
          ? row.metadata
          : JSON.stringify(row.metadata ?? {}),
    });
  }

  return enriched;
}

/**
 * Write Silver records to a Parquet file at the given path.
 */
export async function writeSilverFile(
  filePath: string,
  records: SilverRecord[],
): Promise<void> {
  const schema = new parquet.ParquetSchema({
    eventId: { type: "UTF8" },
    tenantId: { type: "UTF8" },
    deviceId: { type: "UTF8" },
    deviceName: { type: "UTF8" },
    metric: { type: "UTF8" },
    value: { type: "DOUBLE" },
    unit: { type: "UTF8", optional: true },
    timestamp: { type: "TIMESTAMP_MILLIS" },
    receivedAt: { type: "TIMESTAMP_MILLIS" },
    latitude: { type: "DOUBLE", optional: true },
    longitude: { type: "DOUBLE", optional: true },
    metadata: { type: "UTF8" },
  });

  await fs.mkdir(path.dirname(filePath), { recursive: true });

  const writer = await parquet.ParquetWriter.openFile(schema, filePath, {
    compression: "SNAPPY",
  });

  for (const record of records) {
    await writer.appendRow({
      ...record,
      timestamp: new Date(record.timestamp).getTime(),
      receivedAt: new Date(record.receivedAt).getTime(),
      unit: record.unit ?? undefined,
      latitude: record.latitude ?? undefined,
      longitude: record.longitude ?? undefined,
    });
  }

  await writer.close();
}
