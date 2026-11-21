import * as grpc from "@grpc/grpc-js";
import type { Change, Cursor, ScanRecord } from "./index.js";
import { errorCode } from "./retry.js";

export function cursorFromChange(change: Change): Cursor {
  return change.cursor ? { vector: change.cursor } : { revision: change.revision };
}

/** Use the checkpoint from a completed scan. Sharded scans checkpoint at the end. */
export function cursorFromScan(checkpoint: ScanRecord): Cursor {
  if (!checkpoint.checkpoint) throw new TypeError("Expected a scan checkpoint");
  return checkpoint.cursor ? { vector: checkpoint.cursor } : { revision: checkpoint.snapshotRevision };
}

export function needsRescan(error: unknown): boolean {
  const code = errorCode(error);
  return code === grpc.status.OUT_OF_RANGE || code === grpc.status.FAILED_PRECONDITION;
}
