import { startConsumer, getProducer } from "../lib/kafka";
import { ensureTopic, tenantAlertTopic, tenantTelemetryTopic } from "../lib/topics";
import { prisma } from "../lib/prisma";
import { evaluateRule, buildAlertMessage, TelemetryForRules } from "../lib/rules";

const CONSUMER_GROUP = "alerting";

export async function startAlertConsumer(topics: string[]): Promise<void> {
  for (const topic of topics) {
    await ensureTopic(topic);
  }

  await startConsumer({
    groupId: CONSUMER_GROUP,
    topics,
    handler: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      const reading = JSON.parse(raw) as TelemetryForRules;

      // Load active rules for this tenant + metric.
      const rules = await prisma.alertRule.findMany({
        where: {
          tenantId: reading.tenantId,
          metric: reading.metric,
          enabled: true,
        },
      });

      if (rules.length === 0) return;

      const producer = await getProducer();
      const alertTopic = tenantAlertTopic(reading.tenantId);
      await ensureTopic(alertTopic);

      for (const rule of rules) {
        if (!evaluateRule(rule, reading)) continue;

        const alert = buildAlertMessage(rule, reading);

        await producer.send({
          topic: alertTopic,
          messages: [
            {
              key: reading.deviceId,
              value: JSON.stringify(alert),
              headers: {
                severity: alert.severity,
                "rule-id": rule.id,
              },
            },
          ],
        });

        console.log(
          `[alerting] fired rule=${rule.id} severity=${alert.severity} device=${reading.deviceId} value=${reading.value}`
        );
      }
    },
  });

  console.log(`[alerting] started, listening on: ${topics.join(", ")}`);
}