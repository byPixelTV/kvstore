import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

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
type Unary<Request, Response> = (
  request: Request, metadata: grpc.Metadata, options: grpc.CallOptions,
  callback: (err: grpc.ServiceError | null, response?: Response) => void,
) => grpc.ClientUnaryCall;
type Stream<Request, Response> = (request: Request, metadata: grpc.Metadata) => grpc.ClientReadableStream<Response>;

interface RPC extends grpc.Client {
  get: Unary<Key & { consistency: 0 | 1 }, GetResult>;
  set: Unary<SetRequest, SetResult>;
  delete: Unary<Key, DeleteResult>;
  batchWrite: Unary<{ operations: (SetRequest | (Key & { delete: true }))[] }, { results: BatchResult[] }>;
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

export function cursorFromChange(change: Change): Cursor {
  return change.cursor ? { vector: change.cursor } : { revision: change.revision };
}

/** Use the checkpoint from a completed scan. Sharded scans checkpoint at the end. */
export function cursorFromScan(checkpoint: ScanRecord): Cursor {
  if (!checkpoint.checkpoint) throw new TypeError("Expected a scan checkpoint");
  return checkpoint.cursor ? { vector: checkpoint.cursor } : { revision: checkpoint.snapshotRevision };
}

export function needsRescan(error: unknown): boolean {
  if (typeof error !== "object" || error === null || !("code" in error)) return false;
  return error.code === grpc.status.OUT_OF_RANGE || error.code === grpc.status.FAILED_PRECONDITION;
}

export class KVStoreClient {
  private readonly rpc: RPC;
  private readonly metadata = new grpc.Metadata();
  private readonly timeoutMs: number;

  constructor(target: string, { apiKey, plaintext = false, tls, timeoutMs = 5000 }: ClientOptions = {}) {
    if (plaintext && tls) throw new TypeError("plaintext and tls are mutually exclusive");
    if (!!tls?.key !== !!tls?.cert) throw new TypeError("mTLS requires both key and cert");
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) throw new RangeError("timeoutMs must be positive");
    this.timeoutMs = timeoutMs;
    const credentials = plaintext ? grpc.credentials.createInsecure() :
      grpc.credentials.createSsl(tls?.ca, tls?.key, tls?.cert);
    this.rpc = new service.kvstore.v1.KVStore(target, credentials);
    if (apiKey) this.metadata.set("authorization", `Bearer ${apiKey}`);
  }

  close(): void {
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

  get(namespace: string, key: string, consistency: "strong" | "eventual" = "strong"): Promise<GetResult> {
    return this.unary(this.rpc.get, { namespace, key, consistency: consistency === "strong" ? 0 : 1 });
  }

  set(namespace: string, key: string, value: Uint8Array, expiresAtUnixMs = 0): Promise<SetResult> {
    return this.unary(this.rpc.set, { namespace, key, value, expiresAtUnixMs: expiration(expiresAtUnixMs) });
  }

  delete(namespace: string, key: string): Promise<DeleteResult> {
    return this.unary(this.rpc.delete, { namespace, key });
  }

  /** Results are ordered. A transport failure can leave the outcome unknown. */
  async batchWrite(operations: BatchOperation[]): Promise<BatchResult[]> {
    const request = operations.map(op => op.delete
      ? { namespace: op.namespace, key: op.key, delete: true as const }
      : { namespace: op.namespace, key: op.key, value: op.value, expiresAtUnixMs: expiration(op.expiresAtUnixMs ?? 0) });
    return (await this.unary(this.rpc.batchWrite, { operations: request })).results;
  }

  async getJson<T>(namespace: string, key: string): Promise<T | undefined> {
    const result = await this.get(namespace, key);
    return result.found ? JSON.parse(result.value.toString("utf8")) as T : undefined;
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
