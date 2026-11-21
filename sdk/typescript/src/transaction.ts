import type { BatchOperation, GetResult } from "./index.js";

export type TransactionCheck = { namespace: string; key: string } & (
  | { revision: string; value?: never }
  | { value: Uint8Array; revision?: never }
);

export interface TransactionRequest {
  requestId: string;
  checks?: readonly TransactionCheck[];
  operations?: readonly BatchOperation[];
}

export class TransactionConflictError extends Error {
  constructor() {
    super("KVStore transaction precondition failed");
    this.name = "TransactionConflictError";
  }
}

export interface Transaction {
  readonly signal: AbortSignal;
  get(namespace: string, key: string): Promise<Buffer | undefined>;
  getJson<T>(namespace: string, key: string): Promise<T | undefined>;
  set(namespace: string, key: string, value: Uint8Array, expiresAtUnixMs?: number): void;
  setJson<T>(namespace: string, key: string, value: T, expiresAtUnixMs?: number): void;
  delete(namespace: string, key: string): void;
}

export class StagedTransaction implements Transaction {
  private readonly reads = new Map<string, Promise<GetResult>>();
  private readonly readsForCommit: Array<Promise<TransactionCheck>> = [];
  private readonly writes = new Map<string, BatchOperation>();
  private open = true;

  constructor(
    readonly signal: AbortSignal,
    private readonly read: (namespace: string, key: string) => Promise<GetResult>,
  ) {}

  async get(namespace: string, key: string): Promise<Buffer | undefined> {
    this.requireOpen();
    const ref = JSON.stringify([namespace, key]);
    const write = this.writes.get(ref);
    if (write) return write.delete ? undefined : Buffer.from(write.value);
    let read = this.reads.get(ref);
    if (!read) {
      read = this.read(namespace, key);
      this.reads.set(ref, read);
      const check = read.then(record => ({
        namespace, key, revision: record.found ? record.revision : "0",
      }));
      // A callback can catch a failed read, or fail before awaiting it.
      // The same error is observed before commit without an unhandled rejection.
      void check.catch(() => {});
      this.readsForCommit.push(check);
    }
    const record = await read;
    return record.found ? Buffer.from(record.value) : undefined;
  }

  async getJson<T>(namespace: string, key: string): Promise<T | undefined> {
    const value = await this.get(namespace, key);
    return value === undefined ? undefined : JSON.parse(value.toString("utf8")) as T;
  }

  set(namespace: string, key: string, value: Uint8Array, expiresAtUnixMs = 0): void {
    this.requireOpen();
    this.writes.set(JSON.stringify([namespace, key]), { namespace, key, value: Buffer.from(value), expiresAtUnixMs });
  }

  setJson<T>(namespace: string, key: string, value: T, expiresAtUnixMs = 0): void {
    const json = JSON.stringify(value);
    if (json === undefined) throw new TypeError("value has no JSON representation");
    this.set(namespace, key, Buffer.from(json), expiresAtUnixMs);
  }

  delete(namespace: string, key: string): void {
    this.requireOpen();
    this.writes.set(JSON.stringify([namespace, key]), { namespace, key, delete: true });
  }

  async prepare(): Promise<{ checks: TransactionCheck[]; operations: BatchOperation[] }> {
    this.close();
    const checks = await Promise.all(this.readsForCommit);
    this.signal.throwIfAborted();
    return { checks, operations: [...this.writes.values()] };
  }

  close(): void {
    this.open = false;
  }

  private requireOpen(): void {
    this.signal.throwIfAborted();
    if (!this.open) throw new Error("KVStore transaction callback has ended");
  }
}
