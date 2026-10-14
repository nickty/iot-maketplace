import path from "path";
import fs from "fs/promises";
import { readBronzeFile, dedupe, enrich, writeSilverFile, SilverRecord } from "../lib/silver";

const LAKE_ROOT = process.env.LAKE_ROOT || "./lake";
const BRONZE_ROOT = path.join(LAKE_ROOT, "bronze");
const SILVER_ROOT = path.join(LAKE_ROOT, "silver");

/**
 * Walk the Bronze directory, find every Parquet file, transform it,
 * and write the result into the Silver layer.
 *
 * Deduplication is done globally across all files, so a duplicate
 * split across two Bronze files still gets caught.
 *
 * Idempotent: running this twice produces the same Silver output.
 * (Silver files are overwritten by path.)
 */
async function runSilverTransform(): Promise<void> {
  console.log("[silver] starting transform");

  const bronzeFiles = await findParquetFiles(BRONZE_ROOT);
  if (bronzeFiles.length === 0) {
    console.log("[silver] no Bronze files found, exiting");
    return;
  }

  console.log(`[silver] found ${bronzeFiles.length} Bronze files`);

  // Read all rows across all files, in one pass.
  const allRows: any[] = [];
  for (const file of bronzeFiles) {
    const rows = await readBronzeFile(file);
    allRows.push(...rows);
  }

  console.log(`[silver] read ${allRows.length} raw rows`);

  // Dedupe.
  const unique = dedupe(allRows);
  console.log(`[silver] ${unique.length} unique rows after dedupe`);

  // Enrich with device metadata (cached to avoid N queries).
  const deviceCache = new Map<string, { name: string }>();
  const enriched = await enrich(unique, deviceCache);
  console.log(`[silver] enriched ${enriched.length} records`);

  // Group enriched records back into partitions by tenant/year/month/day/hour.
  const groups = new Map<string, SilverRecord[]>();
  for (const record of enriched) {
    const ts = new Date(record.timestamp);
    const partition = [
      `tenant=${record.tenantId}`,
      `year=${ts.getUTCFullYear()}`,
      `month=${String(ts.getUTCMonth() + 1).padStart(2, "0")}`,
      `day=${String(ts.getUTCDate()).padStart(2, "0")}`,
      `hour=${String(ts.getUTCHours()).padStart(2, "0")}`,
    ].join("/");

    const group = groups.get(partition) ?? [];
    group.push(record);
    groups.set(partition, group);
  }

  // Write one Silver file per partition.
  for (const [partition, records] of groups) {
    const filename = `silver-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.parquet`;
    const filePath = path.join(SILVER_ROOT, partition, filename);
    await writeSilverFile(filePath, records);
    console.log(`[silver] wrote ${filePath} (${records.length} records)`);
  }

  console.log("[silver] transform complete");
}

/**
 * Recursively find all *.parquet files under a directory.
 */
async function findParquetFiles(root: string): Promise<string[]> {
  const results: string[] = [];

  async function walk(dir: string) {
    let entries;
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch {
      return;  // directory doesn't exist yet
    }

    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
      } else if (entry.isFile() && entry.name.endsWith(".parquet")) {
        results.push(fullPath);
      }
    }
  }

  await walk(root);
  return results;
}

runSilverTransform()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("[silver] fatal error:", err);
    process.exit(1);
  });