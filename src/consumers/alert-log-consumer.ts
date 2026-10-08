import { startConsumer } from "../lib/kafka";

export async function startAlertLogConsumer(topics: string[]): Promise<void> {
  await startConsumer({
    groupId: "alert-log",
    topics,
    handler: async ({ message }) => {
      const value = message.value?.toString() ?? "";
      console.log(`[alert-log] ${value}`);
    },
  });
  console.log(`[alert-log] listening on: ${topics.join(", ")}`);
}