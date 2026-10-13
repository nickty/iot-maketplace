declare module "parquetjs-lite" {
  export interface ParquetField {
    type: string;
    optional?: boolean;
    repeated?: boolean;
    fields?: Record<string, ParquetField>;
  }

  export type ParquetSchemaType = Record<string, ParquetField>;

  export interface ParquetWriterOptions {
    compression?: "UNCOMPRESSED" | "SNAPPY" | "GZIP" | "LZO" | "BROTLI" | "LZ4" | "ZSTD";
    rowGroupSize?: number;
  }

  export class ParquetWriter {
    static openStream(
      schema: ParquetSchemaType,
      outputStream: NodeJS.WritableStream,
      opts?: ParquetWriterOptions
    ): Promise<ParquetWriter>;

    appendRow(row: Record<string, unknown>): Promise<void>;
    close(): Promise<void>;
  }

  export class ParquetReader {
    static openFile(path: string): Promise<ParquetReader>;
    getCursor(): ParquetCursor;
    close(): Promise<void>;
  }

  export interface ParquetCursor {
    next(): Promise<Record<string, unknown> | null>;
  }

  const parquet: {
    ParquetWriter: typeof ParquetWriter;
    ParquetReader: typeof ParquetReader;
  };

  export default parquet;
}