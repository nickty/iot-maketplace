import { FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import { getProducer } from '../lib/kafka';
import { tenantTelemetryTopic, ensureTopic } from '../lib/topics';

// Schema for what a device sends.
// One message = one reading from one sensor.
const telemetrySchema = z.object({
  metric: z.string().min(1).max(50),       // e.g. "temperature"
  value: z.number().finite(),              // the reading
  unit: z.string().max(20).optional(),     // e.g. "celsius"
  timestamp: z.string().datetime(),        // ISO 8601, when the reading happened
  location: z
    .object({
      lat: z.number().min(-90).max(90),
      lng: z.number().min(-180).max(180),
    })
    .optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

const ingestRoutes: FastifyPluginAsync = async (app) => {
  app.addHook('preHandler', app.authenticateDevice);

  // ─────────────────────────────────────────
  // POST /ingest/telemetry
  // Devices push one reading at a time.
  // ─────────────────────────────────────────
  app.post('/ingest/telemetry', async (request, reply) => {
    const device = request.device!;

    const parsed = telemetrySchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: 'ValidationError',
        details: parsed.error.flatten(),
      });
    }

    const reading = parsed.data;
    const topic = tenantTelemetryTopic(device.tenantId);

    // Ensure the topic exists. Cheap after the first time — cached in
    // the Kafka client's metadata.
    await ensureTopic(topic);

    // The canonical envelope: everything downstream sees the same shape.
    // This is the contract between ingestion and all future processors.
    const envelope = {
      tenantId: device.tenantId,
      deviceId: device.id,
      metric: reading.metric,
      value: reading.value,
      unit: reading.unit ?? null,
      timestamp: reading.timestamp,
      location: reading.location ?? null,
      metadata: reading.metadata ?? {},
      // When we received it — separate from when the device recorded it.
      // These differ when the network is slow. Always store both.
      receivedAt: new Date().toISOString(),
    };

    const producer = await getProducer();

    await producer.send({
      topic,
      // The message key. Kafka uses this to decide which partition.
      // Same key → same partition → guaranteed order.
      // We key by deviceId so all messages from one device arrive in order.
      messages: [
        {
          key: device.id,
          value: JSON.stringify(envelope),
          // Optionally set headers — useful for routing/filtering without
          // deserializing the body.
          headers: {
            'content-type': 'application/json',
            'tenant-id': device.tenantId,
          },
        },
      ],
    });

    return reply.code(202).send({
      status: 'accepted',
      // Include receivedAt so the device knows the server got it.
      receivedAt: envelope.receivedAt,
    });
  });
};

export default ingestRoutes;