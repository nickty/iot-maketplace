import fs from "fs/promises";
import path from "path";

/**
 * Storage backend for the data lake.
 *
 * Currently local filesystem. When you move to S3, add a branch here
 * and swap in the AWS SDK calls. The rest of the codebase doesn't change.
 *
 * The interface is intentionally minimal — only what we need today.
 */

const LAKE_ROOT = process.env.LAKE_ROOT || path.join(process.cwd(), "lake");

export interface WriteResult {
  location: string;   // a display-friendly "path" (URL-like later for S3)
  bytes: number;
}

/**
 * Write a buffer to the lake at the given relative key.
 *
 * The key uses Hive-style partitioning: "bronze/tenant=X/year=Y/.../file.parquet"
 * The storage layer just creates the directories and writes the file.
 */
export async function writeToLake(relativeKey: string, body: Buffer): Promise<WriteResult> {
  const fullPath = path.join(LAKE_ROOT, relativeKey);

  // Recursively create directories. mkdir with recursive:true is
  // idempotent — no error if the directory already exists.
  await fs.mkdir(path.dirname(fullPath), { recursive: true });

  await fs.writeFile(fullPath, body);

  return {
    location: `file://${fullPath}`,
    bytes: body.length,
  };
}

/**
 * Return the root path for the lake. Useful for logging and for
 * DuckDB queries that need an absolute path.
 */
export function getLakeRoot(): string {
  return LAKE_ROOT;
}