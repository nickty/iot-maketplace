import path from "path";
import { aggregateToGold, writeGoldFile } from "../lib/gold";
import { Database } from "duckdb-async";

const LAKE_ROOT = process.env.LAKE_ROOT || "./lake";
const SILVER_ROOT = path.join(LAKE_ROOT, "silver");
const GOLD_ROOT = path.join(LAKE_ROOT, "gold");

async function runGoldProducer(): Promise<void> {
  console.log("[gold] starting aggregation");

  const allRows = await readAllSilverRows();
  if (allRows.length === 0) {
    console.log("[gold] no Silver rows found, exiting");
    return;
  }
  console.log(`[gold] read ${allRows.length} Silver rows`);

  const gold = aggregateToGold(allRows);
  console.log(`[gold] aggregated to ${gold.length} Gold rows`);

  // One Gold file per tenant per day, holding all hourly buckets.
  const groups = new Map<string, typeof gold>();
  for (const record of gold) {
    const ts = new Date(record.hour);
    const partition = [
      `tenant=${record.tenantId}`,
      `year=${ts.getUTCFullYear()}`,
      `month=${String(ts.getUTCMonth() + 1).padStart(2, "0")}`,
      `day=${String(ts.getUTCDate()).padStart(2, "0")}`,
    ].join("/");

    const group = groups.get(partition) ?? [];
    group.push(record);
    groups.set(partition, group);
  }

  for (const [partition, records] of groups) {
    const filename = `gold-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.parquet`;
    const filePath = path.join(GOLD_ROOT, partition, filename);
    await writeGoldFile(filePath, records);
    console.log(`[gold] wrote ${filePath} (${records.length} records)`);
  }

  console.log("[gold] aggregation complete");
}

runGoldProducer()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("[gold] fatal error:", err);
    process.exit(1);
  });



  async function readAllSilverRows(): Promise<any[]> {
  const db = await Database.create(":memory:");
  const normalizedRoot = SILVER_ROOT.replace(/\\/g, "/");
  try {
    const rows = await db.all(
      `SELECT * FROM read_parquet('${normalizedRoot}/**/*.parquet', hive_partitioning = true)`
    );
    return rows;
  } catch (err) {
    // No files yet — the glob finds nothing.
    if (String(err).includes("No files found")) return [];
    throw err;
  } finally {
    await db.close();
  }
}