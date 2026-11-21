import * as grpc from "@grpc/grpc-js";
import { cursorFromChange, cursorFromScan, needsRescan } from "./cursor.js";
import type { Cursor, GetResult, KVStoreClient } from "./index.js";
import { errorCode, pause } from "./retry.js";

type Transport = Pick<KVStoreClient, "scan" | "watch">;

interface Snapshot {
  records: Map<string, GetResult>;
  cursor: Cursor;
}

function expired(record: GetResult, now: number): boolean {
  const expiry = Number(record.expiresAtUnixMs);
  return expiry > 0 && expiry <= now;
}

function transient(error: unknown): boolean {
  const code = errorCode(error);
  return code === grpc.status.UNAVAILABLE || code === grpc.status.DEADLINE_EXCEEDED ||
    code === grpc.status.ABORTED;
}

async function scan(transport: Transport, namespace: string, signal: AbortSignal): Promise<Snapshot> {
  const records = new Map<string, GetResult>();
  let cursor: Cursor | undefined;
  for await (const record of transport.scan(namespace, signal)) {
    if (record.checkpoint) {
      cursor = cursorFromScan(record);
      continue;
    }
    records.set(record.key, {
      found: true, value: record.value, revision: record.revision, expiresAtUnixMs: record.expiresAtUnixMs,
    });
  }
  signal.throwIfAborted();
  if (!cursor) throw new Error("KVStore scan ended without a checkpoint");
  return { records, cursor };
}

/** A completed namespace scan maintained by a resumable Watch. */
export class NamespaceCache {
  private readonly stop = new AbortController();
  private records: Map<string, GetResult>;
  private generation = 1;
  private loaded = true;
  readonly finished: Promise<void>;

  private constructor(
    transport: Transport, namespace: string, snapshot: Snapshot, parent: AbortSignal,
  ) {
    this.records = snapshot.records;
    const signal = AbortSignal.any([parent, this.stop.signal]);
    this.finished = this.follow(transport, namespace, snapshot.cursor, signal).catch(error => {
      if (!signal.aborted) throw error;
    }).finally(() => this.clear());
    // Consumers may use the cache synchronously and choose to inspect finished later.
    void this.finished.catch(() => {});
  }

  static async open(transport: Transport, namespace: string, signal: AbortSignal): Promise<NamespaceCache> {
    const initial = AbortSignal.any([signal, AbortSignal.timeout(10_000)]);
    for (;;) {
      try {
        return new NamespaceCache(transport, namespace, await scan(transport, namespace, initial), signal);
      } catch (error) {
        initial.throwIfAborted();
        if (!transient(error)) throw error;
        await pause(25, initial);
      }
    }
  }

  get version(): number {
    return this.generation;
  }

  get ready(): boolean {
    return this.loaded;
  }

  get(key: string): GetResult | undefined {
    const record = this.records.get(key);
    if (!record) return undefined;
    if (expired(record, Date.now())) {
      this.records.delete(key);
      this.generation++;
      return undefined;
    }
    return { ...record, value: Buffer.from(record.value) };
  }

  snapshot(): ReadonlyMap<string, GetResult> {
    const result = new Map<string, GetResult>();
    const now = Date.now();
    for (const [key, record] of this.records) {
      if (expired(record, now)) {
        this.records.delete(key);
        this.generation++;
      } else {
        result.set(key, { ...record, value: Buffer.from(record.value) });
      }
    }
    return result;
  }

  close(): void {
    this.stop.abort();
    this.clear();
  }

  private clear(): void {
    if (this.loaded) {
      this.records.clear();
      this.loaded = false;
      this.generation++;
    }
  }

  private async follow(transport: Transport, namespace: string, after: Cursor, signal: AbortSignal): Promise<void> {
    let rescan = false;
    while (!signal.aborted) {
      try {
        if (rescan) {
          const next = await scan(transport, namespace, signal);
          this.records = next.records;
          this.loaded = true;
          this.generation++;
          after = next.cursor;
          rescan = false;
        }
        for await (const change of transport.watch(namespace, { after, signal })) {
          signal.throwIfAborted();
          if (!change.checkpoint) {
            if (change.deleted) {
              if (this.records.delete(change.key)) this.generation++;
            } else if (this.records.get(change.key)?.revision !== change.revision) {
              this.records.set(change.key, {
                found: true, value: change.value, revision: change.revision, expiresAtUnixMs: change.expiresAtUnixMs,
              });
              this.generation++;
            }
          }
          after = cursorFromChange(change);
        }
      } catch (error) {
        signal.throwIfAborted();
        if (needsRescan(error)) {
          this.clear();
          rescan = true;
        } else if (!transient(error)) {
          throw error;
        }
      }
      await pause(25, signal);
    }
  }
}
