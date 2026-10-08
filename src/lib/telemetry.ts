import { randomUUID } from "crypto";
import { Prisma } from "@prisma/client";
import { prisma } from "./prisma";

/**
 * Input shape: what a consumer receives from Kafka.
 */
export interface TelemetryInput {
  tenantId: string;
  deviceId: string;
  metric: string;
  value: number;
  unit: string | null;
  timestamp: string; // ISO 8601
  receivedAt: string; // ISO 8601
  location: { lat: number; lng: number } | null;
  metadata: Record<string, unknown>;
}

/**
 * Persist a batch of telemetry events idempotently.
 *
 * For each event:
 *   1. Try to INSERT into processed_events.
 *   2. If the insert succeeds (new event), INSERT into telemetry.
 *   3. If the insert fails with a unique-constraint violation,
 *      the event was already processed — skip it.
 *
 * All events in the batch run inside ONE transaction, so if any
 * step fails, the whole batch rolls back and Kafka will redeliver.
 */
export async function persistTelemetryBatch(
  consumer: string,
  events: Array<TelemetryInput & { eventId: string }>,
): Promise<{ written: number; skipped: number }> {
  let written = 0;
  let skipped = 0;

  await prisma.$transaction(async (tx) => {
    for (const event of events) {
      try {
        // Step 1: mark the event as processed.
        await tx.processedEvent.create({
          data: {
            eventId: event.eventId,
            consumer,
          },
        });
      } catch (err) {
        // Prisma throws P2002 on unique constraint violation.
        // That means we've already processed this event.
        if (
          err instanceof Prisma.PrismaClientKnownRequestError &&
          err.code === "P2002"
        ) {
          skipped++;
          continue;
        }
        // Any other error is a real problem.
        throw err;
      }

      // Step 2: insert the telemetry row.
      const newRowId = randomUUID();
      await tx.telemetry.create({
        data: {
          id: newRowId,
          tenantId: event.tenantId,
          deviceId: event.deviceId,
          metric: event.metric,
          value: event.value,
          unit: event.unit,
          timestamp: new Date(event.timestamp),
          receivedAt: new Date(event.receivedAt),
          latitude: event.location?.lat ?? null,
          longitude: event.location?.lng ?? null,
          metadata: event.metadata as Prisma.InputJsonValue,
        },
      });

      if (event.location) {
        await tx.$executeRaw`
    UPDATE telemetry
    SET location = ST_SetSRID(ST_MakePoint(${event.location.lng}, ${event.location.lat}), 4326)::geography
    WHERE id = ${newRowId}::uuid AND timestamp = ${new Date(event.timestamp)}
  `;
      }

      written++;
    }
  });

  return { written, skipped };
}
