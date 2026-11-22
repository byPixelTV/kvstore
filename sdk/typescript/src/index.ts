import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { LeaseManager, type LockOptions } from "./lease.js";
import { NamespaceCache } from "./namespace-cache.js";
import { errorCode, pause } from "./retry.js";
import {
  StagedTransaction, TransactionConflictError, type Transaction, type TransactionCheck, type TransactionRequest,
} from "./transaction.js";

export { cursorFromChange, cursorFromScan, needsRescan } from "./cursor.js";
export { LeaseLostError, defaultLockTtlMs, leaseNamespace } from "./lease.js";
export type { LockOptions } from "./lease.js";
export { NamespaceCache } from "./namespace-cache.js";
export { TransactionConflictError } from "./transaction.js";
export type { Transaction, TransactionCheck, TransactionRequest } from "./transaction.js";

export interface ClientOptions {
  apiKey?: string;
  plaintext?: boolean;
  tls?: { ca?: Buffer; key?: Buffer; cert?: Buffer };
  timeoutMs?: number;
}

export interface Cursor {
  revision?: string;
  vector?: string;
}

export interface WatchOptions {
  key?: string;
  after?: Cursor;
  signal?: AbortSignal;
}

export interface GetResult {
  found: boolean;
  value: Buffer;
  revision: string;
  expiresAtUnixMs: string;
}

export type SetResult = Pick<GetResult, "revision" | "expiresAtUnixMs">;
export interface DeleteResult { deleted: boolean; revision: string }
export interface Head {
  revision: string;
  oldestAvailableRevision: string;
  cursor: string;
  supportsExpiration: boolean;
}

export interface Change {
  namespace: string;
  key: string;
  revision: string;
  value: Buffer;
  deleted: boolean;
  checkpoint: boolean;
  cursor: string;
  shard: number;
  expiresAtUnixMs: string;
}

export interface ScanRecord {
  snapshotRevision: string;
  key: string;
  value: Buffer;
  revision: string;
  checkpoint: boolean;
  cursor: string;
  shard: number;
  expiresAtUnixMs: string;
}

interface Key { namespace: string; key: string }
export type BatchOperation = Key & (
  | { value: Uint8Array; expiresAtUnixMs?: number; delete?: false }
  | { delete: true }
);
export interface BatchResult extends DeleteResult { grpcCode: number; error: string }

type Vector = { useVectorCursor: true };
type SetRequest = Key & { value: Uint8Array; expiresAtUnixMs: string };
type WireOperation = SetRequest | (Key & { delete: true });
type Unary<Request, Response> = (
  request: Request, metadata: grpc.Metadata, options: grpc.CallOptions,
  callback: (err: grpc.ServiceError | null, response?: Response) => void,
) => grpc.ClientUnaryCall;
type Stream<Request, Response> = (request: Request, metadata: grpc.Metadata) => grpc.ClientReadableStream<Response>;

interface RPC extends grpc.Client {
  get: Unary<Key & { consistency: 0 | 1 }, GetResult>;
  set: Unary<SetRequest, SetResult>;
  delete: Unary<Key, DeleteResult>;
  batchWrite: Unary<{ operations: WireOperation[] }, { results: BatchResult[] }>;
  transact: Unary<{ requestId: string; checks: TransactionCheck[]; operations: WireOperation[] }, { committed: boolean }>;
  head: Unary<Vector, Head>;
  watch: Stream<Key & Vector & { afterRevision: string; cursor: string }, Change>;
  scan: Stream<Vector & { namespace: string }, ScanRecord>;
}

const definition = loadSync(fileURLToPath(new URL("./kvstore.proto", import.meta.url)), {
  defaults: true,
  longs: String,
});
const service = grpc.loadPackageDefinition(definition) as unknown as {
  kvstore: { v1: { KVStore: new (target: string, credentials: grpc.ChannelCredentials) => RPC } };
};

function expiration(ms: number): string {
  if (!Number.isSafeInteger(ms) || ms < 0) {
    throw new RangeError("expiresAtUnixMs must be a nonnegative safe integer");
  }
  return String(ms);
}

function encodeOperations(operations: readonly BatchOperation[]): WireOperation[] {
  return operations.map(op => op.delete
    ? { namespace: op.namespace, key: op.key, delete: true }
    : { namespace: op.namespace, key: op.key, value: op.value, expiresAtUnixMs: expiration(op.expiresAtUnixMs ?? 0) });
}

async function* frames<T>(open: () => grpc.ClientReadableStream<T>, signal?: AbortSignal): AsyncGenerator<T> {
  signal?.throwIfAborted();
  const stream = open();
  const abort = () => stream.cancel();
  signal?.addEventListener("abort", abort, { once: true });
  try {
    for await (const frame of stream) {
      yield frame;
    }
  } finally {
    signal?.removeEventListener("abort", abort);
    stream.cancel();
  }
}

export class KVStoreClient {
  private readonly rpc: RPC;
  private readonly metadata = new grpc.Metadata();
  private readonly timeoutMs: number;
  private readonly lifetime = new AbortController();
  private readonly leases: LeaseManager;

  constructor(target: string, { apiKey, plaintext = false, tls, timeoutMs = 5000 }: ClientOptions = {}) {
    if (plaintext && tls) throw new TypeError("plaintext and tls are mutually exclusive");
    if (!!tls?.key !== !!tls?.cert) throw new TypeError("mTLS requires both key and cert");
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) throw new RangeError("timeoutMs must be positive");
    this.timeoutMs = timeoutMs;
    const credentials = plaintext ? grpc.credentials.createInsecure() :
      grpc.credentials.createSsl(tls?.ca, tls?.key, tls?.cert);
    this.rpc = new service.kvstore.v1.KVStore(target, credentials);
    if (apiKey) this.metadata.set("authorization", `Bearer ${apiKey}`);
    this.leases = new LeaseManager(
      this.lifetime.signal,
      (namespace, key, signal) => this.readForTransaction(namespace, key, signal),
      (checks, operations, signal) => this.transactWithRetry(checks, operations, signal),
    );
  }

  close(): void {
    this.lifetime.abort();
    this.rpc.close();
  }

  private unary<Request, Response>(
    rpc: Unary<Request, Response>, request: Request, signal?: AbortSignal,
  ): Promise<Response> {
    return new Promise((resolve, reject) => {
      signal?.throwIfAborted();
      const abort = () => call.cancel();
      const call = rpc.call(this.rpc, request, this.metadata, { deadline: Date.now() + this.timeoutMs }, (err, response) => {
        signal?.removeEventListener("abort", abort);
        if (err) reject(err);
        else resolve(response!);
      });
      signal?.addEventListener("abort", abort, { once: true });
    });
  }

  get(namespace: string, key: string, consistency: "strong" | "eventual" = "strong", signal?: AbortSignal): Promise<GetResult> {
    return this.unary(this.rpc.get, { namespace, key, consistency: consistency === "strong" ? 0 : 1 }, signal);
  }

  set(namespace: string, key: string, value: Uint8Array, expiresAtUnixMs = 0): Promise<SetResult> {
    return this.unary(this.rpc.set, { namespace, key, value, expiresAtUnixMs: expiration(expiresAtUnixMs) });
  }

  delete(namespace: string, key: string): Promise<DeleteResult> {
    return this.unary(this.rpc.delete, { namespace, key });
  }

  /** Results are ordered. A transport failure can leave the outcome unknown. */
  async batchWrite(operations: BatchOperation[]): Promise<BatchResult[]> {
    return (await this.unary(this.rpc.batchWrite, { operations: encodeOperations(operations) })).results;
  }

  /** Retry an unknown outcome with the same request ID and exact input. */
  async transact(request: TransactionRequest, signal?: AbortSignal): Promise<boolean> {
    const reply = await this.unary(this.rpc.transact, {
      requestId: request.requestId,
      checks: [...request.checks ?? []],
      operations: encodeOperations(request.operations ?? []),
    }, signal);
    return reply.committed;
  }

  /** Run the callback once, then atomically validate its read set and apply its writes. */
  async transaction<T>(callback: (transaction: Transaction) => T | Promise<T>, signal?: AbortSignal): Promise<T> {
    const active = this.leases.signal(signal);
    active.throwIfAborted();
    const transaction = new StagedTransaction(active, (namespace, key) => this.readForTransaction(namespace, key, active));
    try {
      const result = await callback(transaction);
      const { checks, operations } = await transaction.prepare();
      checks.unshift(...this.leases.checks);
      if (checks.length + operations.length > 0 && !(await this.transactWithRetry(checks, operations, active))) {
        throw new TransactionConflictError();
      }
      return result;
    } finally {
      transaction.close();
    }
  }

  /** Keep an exclusive, renewable lease for callback-scoped transactions. */
  withKeyLock<T>(
    namespace: string, key: string, callback: (signal: AbortSignal) => T | Promise<T>, options: LockOptions = {},
  ): Promise<T> {
    return this.leases.withKeyLock(namespace, key, callback, options);
  }

  /** Load a local snapshot and maintain it through changefeed replay. */
  loadNamespaceCache(namespace: string, signal?: AbortSignal): Promise<NamespaceCache> {
    const active = signal ? AbortSignal.any([signal, this.lifetime.signal]) : this.lifetime.signal;
    return NamespaceCache.open(this, namespace, active);
  }

  private async readForTransaction(namespace: string, key: string, signal: AbortSignal): Promise<GetResult> {
    const bounded = AbortSignal.any([signal, AbortSignal.timeout(this.timeoutMs)]);
    for (;;) {
      try {
        return await this.get(namespace, key, "strong", bounded);
      } catch (error) {
        bounded.throwIfAborted();
        if (errorCode(error) !== grpc.status.UNAVAILABLE) throw error;
      }
      await pause(25, bounded);
    }
  }

  private async transactWithRetry(
    checks: TransactionCheck[], operations: BatchOperation[], signal: AbortSignal,
  ): Promise<boolean> {
    signal.throwIfAborted();
    const request = { requestId: randomUUID(), checks, operations };
    const bounded = AbortSignal.any([signal, AbortSignal.timeout(10_000)]);
    try {
      for (;;) {
        try {
          return await this.transact(request, bounded);
        } catch (error) {
          bounded.throwIfAborted();
          const code = errorCode(error);
          if (code !== grpc.status.UNAVAILABLE && code !== grpc.status.DEADLINE_EXCEEDED && code !== grpc.status.ABORTED) {
            throw error;
          }
        }
        await pause(25, bounded);
      }
    } catch (error) {
      if (bounded.aborted) throw new Error(`KVStore transaction ${request.requestId} outcome unknown`, { cause: error });
      throw error;
    }
  }

  async getJson<T>(namespace: string, key: string, options: { emptyAsMissing?: boolean } = {}): Promise<T | undefined> {
    const result = await this.get(namespace, key);
    if (!result.found || (options.emptyAsMissing && result.value.length === 0)) return undefined;
    return JSON.parse(result.value.toString("utf8")) as T;
  }

  async setJson<T>(namespace: string, key: string, value: T, expiresAtUnixMs = 0): Promise<SetResult> {
    const json = JSON.stringify(value);
    if (json === undefined) throw new TypeError("value has no JSON representation");
    return this.set(namespace, key, Buffer.from(json), expiresAtUnixMs);
  }

  head(signal?: AbortSignal): Promise<Head> {
    return this.unary(this.rpc.head, { useVectorCursor: true }, signal);
  }

  scan(namespace: string, signal?: AbortSignal): AsyncGenerator<ScanRecord> {
    return frames(() => this.rpc.scan({ namespace, useVectorCursor: true }, this.metadata), signal);
  }

  /** Pass a cursor to replay changes. With no cursor the stream starts at Head. */
  async *watch(namespace: string, options: WatchOptions = {}): AsyncGenerator<Change> {
    const { key = "", signal } = options;
    let after = options.after;
    if (!after) {
      const { cursor, revision } = await this.head(signal);
      after = { vector: cursor, revision };
    }
    const vector = after.vector ?? "";
    yield* frames(() => this.rpc.watch({
      namespace, key, afterRevision: vector ? "0" : after.revision ?? "0", cursor: vector, useVectorCursor: true,
    }, this.metadata), signal);
  }

  /** Iterate over committed changes. Use watch when you need cursor checkpoints. */
  async *onChange(namespace: string, options: WatchOptions = {}): AsyncGenerator<Change> {
    for await (const change of this.watch(namespace, options)) {
      if (!change.checkpoint) yield change;
    }
  }
}
