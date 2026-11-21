package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc/codes"
)

const (
	importBatchOperations = 128
	importBatchBytes      = 192 << 10
)

// Compare against one consistent read per namespace. The target must be
// reserved for the importer for both planning and replay.
func planSnapshotImport(ctx context.Context, client kv.KVStoreClient, manifest snapshotManifest, index snapshotIndex, showConflicts bool) (totals, map[string]map[string]bool, error) {
	stats := totals{expired: manifest.Expired, skippedExpiring: manifest.SkippedExpiring}
	seen := make(map[string]map[string]bool)
	for _, namespace := range manifest.Namespaces {
		expected := index[namespace]
		if len(expected) == 0 {
			continue
		}
		present := make(map[string]bool)
		seen[namespace] = present
		call, cancel := context.WithTimeout(ctx, 5*time.Minute)
		err := scanTargetNamespace(call, client, namespace, len(expected), func(item *kv.ScanResponse) {
			key := item.GetKey()
			want, found := expected[key]
			if !found {
				return
			}
			present[key] = true
			if want == expectRecord(item.GetValue(), item.GetExpiresAtUnixMs()) {
				stats.unchanged++
			} else {
				stats.conflicts++
				printDifference(showConflicts, "conflict", namespace, key)
			}
		})
		cancel()
		if err != nil {
			return stats, nil, fmt.Errorf("check destination namespace %q: %w", namespace, err)
		}
		now := time.Now().UnixMilli()
		for key, want := range expected {
			if present[key] {
				continue
			}
			if deadlinePassed(want.expiresAtUnixMS, now) {
				stats.expiredSinceValidation++
			} else {
				stats.newKeys++
			}
		}
	}
	return stats, seen, nil
}

func applySnapshot(ctx context.Context, client kv.KVStoreClient, input io.Reader, manifest snapshotManifest, index snapshotIndex, present map[string]map[string]bool, stats *totals) error {
	stats.expiredSinceValidation = 0 // recheck deadlines as records are sent
	batch := make([]*kv.BatchWriteOperation, 0, importBatchOperations)
	batchBytes := 0
	flush := func() error {
		if err := writeImportBatch(ctx, client, batch, stats); err != nil {
			return err
		}
		clear(batch)
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	var processed int64
	nextReport := time.Now().Add(2 * time.Second)
	replayed, err := walkSnapshot(ctx, input, func(row *kv.SetRequest) error {
		// Reject modifications made after snapshot validation before they can
		// reach the destination, including duplicate records.
		expected, found := index[row.Namespace][row.Key]
		if !found || expected != expectRecord(row.Value, row.GetExpiresAtUnixMs()) {
			return errors.New("snapshot changed after validation")
		}
		delete(index[row.Namespace], row.Key)
		processed++
		now := time.Now()
		if !now.Before(nextReport) {
			fmt.Printf("import progress: scanned=%d/%d acknowledged=%d already_matching=%d expired=%d\n",
				processed, manifest.Records, stats.written, stats.unchanged, stats.expiredSinceValidation)
			nextReport = now.Add(2 * time.Second)
		}
		if present[row.Namespace][row.Key] {
			return nil
		}
		if deadlinePassed(row.GetExpiresAtUnixMs(), now.UnixMilli()) {
			stats.expiredSinceValidation++
			return nil
		}
		recordBytes := importRecordBytes(row)
		if len(batch) == importBatchOperations || batchBytes+recordBytes > importBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		if recordBytes > importBatchBytes {
			if deadlinePassed(row.GetExpiresAtUnixMs(), time.Now().UnixMilli()) {
				stats.expiredSinceValidation++
				return nil
			}
			call, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := writeRecord(call, client, row); err != nil {
				return err
			}
			stats.written++
			return nil
		}
		batch = append(batch, &kv.BatchWriteOperation{
			Namespace: row.Namespace, Key: row.Key, Value: row.Value, ExpiresAtUnixMs: row.GetExpiresAtUnixMs(),
		})
		batchBytes += recordBytes
		return nil
	})
	if err != nil {
		return err
	}
	if replayed.SHA256 != manifest.SHA256 {
		return errors.New("snapshot changed after validation")
	}
	return flush()
}

func importRecordBytes(row *kv.SetRequest) int {
	// The backend Raft envelope uses JSON with base64 values. Include string
	// escaping as well as the raw wire limits when deciding where to split.
	namespace, _ := json.Marshal(row.Namespace)
	key, _ := json.Marshal(row.Key)
	return 128 + len(namespace) + len(key) + base64.StdEncoding.EncodedLen(len(row.Value))
}

func writeImportBatch(ctx context.Context, client kv.KVStoreClient, operations []*kv.BatchWriteOperation, stats *totals) error {
	now := time.Now().UnixMilli()
	live := make([]*kv.BatchWriteOperation, 0, len(operations))
	for _, operation := range operations {
		if deadlinePassed(operation.GetExpiresAtUnixMs(), now) {
			stats.expiredSinceValidation++
			continue
		}
		live = append(live, operation)
	}
	if len(live) == 0 {
		return nil
	}
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	reply, err := client.BatchWrite(call, &kv.BatchWriteRequest{Operations: live})
	if err != nil {
		return fmt.Errorf("batch outcome may be unknown; rerun against the same snapshot to reconcile: %w", err)
	}
	results := reply.GetResults()
	if len(results) != len(live) {
		return errors.New("destination returned an incomplete batch response; rerun against the same snapshot to reconcile")
	}
	var firstErr error
	for i, result := range results {
		if result != nil && codes.Code(result.GetGrpcCode()) == codes.OK && result.GetRevision() != 0 && !result.GetDeleted() {
			stats.written++
			continue
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("batch item %d was not acknowledged (gRPC code %d); rerun against the same snapshot to reconcile",
				i+1, result.GetGrpcCode())
		}
	}
	return firstErr
}
