import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { BatchOperation, GetResult } from "./index.js";
import { pause } from "./retry.js";
import type { TransactionCheck } from "./transaction.js";

export const leaseNamespace = "kvstore-leases";
export const defaultLockTtlMs = 10_000;
const transactionHorizonMs = 3_000;

export class LeaseLostError extends Error {
  constructor(options?: ErrorOptions) {
    super("KVStore distributed lock was lost", options);
    this.name = "LeaseLostError";
  }
}

export interface LockOptions {
  ttlMs?: number;
  signal?: AbortSignal;
}

interface HeldLock {
  key: string;
  value: Buffer;
}

interface LeaseScope {
  signal: AbortSignal;
  locks: HeldLock[];
}

function ownerCheck(lock: HeldLock): TransactionCheck {
  return { namespace: leaseNamespace, key: lock.key, value: lock.value };
}

export class LeaseManager {
  private readonly held = new AsyncLocalStorage<LeaseScope>();

  constructor(
    private readonly lifetime: AbortSignal,
    private readonly read: (namespace: string, key: string, signal: AbortSignal) => Promise<GetResult>,
    private readonly transact: (checks: TransactionCheck[], writes: BatchOperation[], signal: AbortSignal) => Promise<boolean>,
  ) {}

  get checks(): TransactionCheck[] {
    return this.held.getStore()?.locks.map(ownerCheck) ?? [];
  }

  signal(signal?: AbortSignal): AbortSignal {
    const held = this.held.getStore();
    const active = held?.signal ?? this.lifetime;
    return signal ? AbortSignal.any([active, signal]) : active;
  }

  async withKeyLock<T>(
    namespace: string,
    key: string,
    callback: (signal: AbortSignal) => T | Promise<T>,
    { signal, ttlMs = defaultLockTtlMs }: LockOptions,
  ): Promise<T> {
    if (!namespace || !key || !Number.isSafeInteger(ttlMs + Date.now()) || !Number.isSafeInteger(ttlMs) || ttlMs < 5000) {
      throw new RangeError("a lock requires a namespace, a key and a TTL of at least 5000 ms");
    }
    const active = this.signal(signal);
    active.throwIfAborted();
    const locks = this.held.getStore()?.locks ?? [];
    // Matches the Go and Kotlin wire format exactly.
    const encoded = Buffer.from(namespace).toString("base64url") + ":" + Buffer.from(key).toString("base64url");
    if (locks.some(lock => lock.key === encoded)) {
      const result = await this.held.run({ signal: active, locks }, () => callback(active));
      active.throwIfAborted();
      return result;
    }

    const lock = { key: encoded, value: Buffer.from(randomUUID()) };
    const acquisition = AbortSignal.any([active, AbortSignal.timeout(defaultLockTtlMs)]);
    let expiresAt = await this.acquire(lock, ttlMs, acquisition);
    const completion = new AbortController();
    const loss = new AbortController();
    const owned = AbortSignal.any([active, completion.signal, loss.signal]);
    const renewal = this.renew(lock, ttlMs, expiresAt, owned, next => { expiresAt = next; }).catch(error => {
      if (!owned.aborted) loss.abort(error instanceof LeaseLostError ? error : new LeaseLostError({ cause: error }));
    });

    const errors: unknown[] = [];
    let result!: T;
    try {
      if (Date.now() >= expiresAt) throw new LeaseLostError();
      owned.throwIfAborted();
      result = await this.held.run({ signal: owned, locks: [lock, ...locks] }, () => callback(owned));
      owned.throwIfAborted();
      if (Date.now() >= expiresAt) throw new LeaseLostError();
    } catch (error) {
      errors.push(error);
    } finally {
      completion.abort();
      await renewal;
      try {
        const release = AbortSignal.any([this.lifetime, AbortSignal.timeout(5000)]);
        if (!(await this.writeIfOwned(lock, 0, release)) &&
            !errors.some(error => error instanceof LeaseLostError)) {
          errors.push(new LeaseLostError());
        }
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length > 1) throw new AggregateError(errors, "KVStore distributed lock callback and release failed");
    if (errors.length === 1) throw errors[0];
    return result;
  }

  private async acquire(lock: HeldLock, ttlMs: number, signal: AbortSignal): Promise<number> {
    const absent = { namespace: leaseNamespace, key: lock.key, revision: "0" };
    for (;;) {
      const existing = await this.read(leaseNamespace, lock.key, signal);
      if (!existing.found) {
        const expiresAt = Date.now() + ttlMs;
        if (await this.write(lock, absent, expiresAt, signal)) return expiresAt;
      }
      await pause(25, signal);
    }
  }

  private async renew(
    lock: HeldLock, ttlMs: number, expiresAt: number, signal: AbortSignal, renewed: (expiry: number) => void,
  ): Promise<void> {
    // A conditional write must observe a lease valid beyond the server's
    // transaction horizon. Renew early enough to leave room for a retry.
    const interval = Math.floor(Math.min(ttlMs / 3, (ttlMs - transactionHorizonMs) / 2));
    while (!signal.aborted) {
      // Cap the timer to Node's signed 32-bit timer range.
      await pause(Math.min(0x7fffffff, Math.max(0, expiresAt - ttlMs + interval - Date.now())), signal);
      const remaining = expiresAt - Date.now();
      if (remaining <= 0) throw new LeaseLostError();
      const renewal = AbortSignal.any([signal, AbortSignal.timeout(Math.min(remaining, defaultLockTtlMs))]);
      const next = Date.now() + ttlMs;
      if (!(await this.writeIfOwned(lock, next, renewal))) throw new LeaseLostError();
      expiresAt = next;
      renewed(next);
    }
  }

  private async writeIfOwned(lock: HeldLock, expiry: number, signal: AbortSignal): Promise<boolean> {
    const check = ownerCheck(lock);
    for (;;) {
      if (await this.write(lock, check, expiry, signal)) return true;
      // Another transaction may temporarily reserve the lease key. Only a
      // strong read of a different owner (or absence) confirms that it is lost.
      const existing = await this.read(leaseNamespace, lock.key, signal);
      if (!existing.found || !existing.value.equals(lock.value)) return false;
      await pause(25, signal);
    }
  }

  private write(lock: HeldLock, check: TransactionCheck, expiry: number, signal: AbortSignal): Promise<boolean> {
    const key = { namespace: leaseNamespace, key: lock.key };
    const operation: BatchOperation = expiry === 0
      ? { ...key, delete: true }
      : { ...key, value: lock.value, expiresAtUnixMs: expiry };
    return this.transact([check], [operation], signal);
  }
}
