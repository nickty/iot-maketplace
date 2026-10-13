import parquet from "parquetjs-lite";
import { PassThrough } from "stream";

/**
 * Schema for a Bronze telemetry record.
 *
 * We store everything the envelope contains, plus receivedAt.
 * Bronze is intentionally wide — schemas are cheap in Parquet.
 */
const bronzeSchema = new parquet.ParquetSchema({
  tenantId:   { type: "UTF8" },
  deviceId:   { type: "UTF8" },
  metric:     { type: "UTF8" },
  value:      { type: "DOUBLE" },
  unit:       { type: "UTF8", optional: true },
  timestamp:  { type: "TIMESTAMP_MILLIS" },
  receivedAt: { type: "TIMESTAMP_MILLIS" },
  latitude:   { type: "DOUBLE", optional: true },
  longitude:  { type: "DOUBLE", optional: true },
  metadata:   { type: "UTF8" },     // JSON-stringified metadata
});

export interface BronzeRecord {
  tenantId: string;
  deviceId: string;
  metric: string;
  value: number;
  unit: string | null;
  timestamp: string;
  receivedAt: string;
  latitude: number | null;
  longitude: number | null;
  metadata: Record<string, unknown>;
}

/**
 * Serialize an array of Bronze records into an in-memory Parquet buffer.
 *
 * Parquet writers expect to write to a stream. We use PassThrough to
 * collect the bytes and return a Buffer.
 */
export async function recordsToParquetBuffer(records: BronzeRecord[]): Promise<Buffer> {
  const outputStream = new PassThrough();
  const chunks: Buffer[] = [];

  outputStream.on("data", (chunk: Buffer) => chunks.push(chunk));

  const finished = new Promise<void>((resolve, reject) => {
    outputStream.on("end", () => resolve());
    outputStream.on("error", reject);
  });

  const writer = await parquet.ParquetWriter.openStream(bronzeSchema, outputStream, {
    compression: "SNAPPY",
  });

  for (const record of records) {
    await writer.appendRow({
      tenantId: record.tenantId,
      deviceId: record.deviceId,
      metric: record.metric,
      value: record.value,
      unit: record.unit ?? undefined,
      timestamp: new Date(record.timestamp).getTime(),
      receivedAt: new Date(record.receivedAt).getTime(),
      latitude: record.latitude ?? undefined,
      longitude: record.longitude ?? undefined,
      metadata: JSON.stringify(record.metadata),
    });
  }

  // writer.close() ends the underlying stream, triggering 'end'.
  await writer.close();
  await finished;

  return Buffer.concat(chunks);
}