package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type batchImportTarget struct {
	kv.KVStoreClient
	batches int
	sets    int
	result  *kv.BatchWriteResponse
	items   []*kv.BatchWriteOperation
}

func (c *batchImportTarget) BatchWrite(_ context.Context, req *kv.BatchWriteRequest, _ ...grpc.CallOption) (*kv.BatchWriteResponse, error) {
	c.batches++
	c.items = append(c.items, req.GetOperations()...)
	if c.result != nil {
		return c.result, nil
	}
	res := &kv.BatchWriteResponse{}
	for range req.GetOperations() {
		res.Results = append(res.Results, &kv.BatchWriteResult{Revision: 1})
	}
	return res, nil
}

func (c *batchImportTarget) Set(_ context.Context, row *kv.SetRequest, _ ...grpc.CallOption) (*kv.SetResponse, error) {
	c.sets++
	return &kv.SetResponse{Revision: 1, ExpiresAtUnixMs: row.GetExpiresAtUnixMs()}, nil
}

func TestSnapshotApplyBatchesOnlyMissingLiveRecords(t *testing.T) {
	const namespace = "store"
	deadline := time.Now().Add(time.Hour).UnixMilli()
	var rows []*kv.SetRequest
	for i := range importBatchOperations + 1 {
		rows = append(rows, &kv.SetRequest{Namespace: namespace, Key: fmt.Sprintf("row-%d", i), ExpiresAtUnixMs: deadline})
	}
	rows = append(rows,
		&kv.SetRequest{Namespace: namespace, Key: "existing"},
		&kv.SetRequest{Namespace: namespace, Key: "large", Value: make([]byte, 170<<10)},
		&kv.SetRequest{Namespace: namespace, Key: "expired", ExpiresAtUnixMs: 1},
	)
	fixture := snapshotFixture(t, make(map[string]struct{}), rows...)
	manifest, index, err := loadSnapshot(t.Context(), bytes.NewReader(fixture))
	if err != nil {
		t.Fatal(err)
	}
	target := &batchImportTarget{}
	stats := totals{unchanged: 1}
	present := map[string]map[string]bool{namespace: {"existing": true}}
	if err := applySnapshot(t.Context(), target, bytes.NewReader(fixture), manifest, index, present, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.written != importBatchOperations+2 || stats.expiredSinceValidation != 1 || target.batches != 2 || target.sets != 1 {
		t.Fatalf("unexpected result: stats=%+v batches=%d sets=%d", stats, target.batches, target.sets)
	}
	for _, item := range target.items {
		if item.GetExpiresAtUnixMs() != deadline {
			t.Fatal("batch changed an expiration deadline")
		}
	}
}

func TestSnapshotBatchCountsPartialAckAndStops(t *testing.T) {
	operations := []*kv.BatchWriteOperation{{Key: "one"}, {Key: "two"}, {Key: "three"}}
	target := &batchImportTarget{result: &kv.BatchWriteResponse{Results: []*kv.BatchWriteResult{
		{Revision: 1}, {GrpcCode: uint32(codes.Unavailable)}, {Revision: 2},
	}}}
	var stats totals
	err := writeImportBatch(t.Context(), target, operations, &stats)
	if err == nil || stats.written != 2 || target.batches != 1 {
		t.Fatalf("expected two acknowledgements and a stopped batch, got %+v, %v", stats, err)
	}
}

func TestSnapshotPlanComparesValuesAndDeadlines(t *testing.T) {
	const namespace = "store"
	target := scanTarget{items: []*kv.ScanResponse{
		{Key: "same", Value: []byte("value")},
		{Key: "wrong-deadline", Value: []byte("value")},
		{Key: "extra", Value: []byte("value")},
		{Checkpoint: true, Cursor: "vector-checkpoint"},
	}}
	index := snapshotIndex{namespace: {
		"same":           expectRecord([]byte("value"), 0),
		"wrong-deadline": expectRecord([]byte("value"), time.Now().Add(time.Hour).UnixMilli()),
		"missing":        expectRecord([]byte("value"), 0),
		"expired":        expectRecord([]byte("value"), 1),
	}}
	manifest := snapshotManifest{Namespaces: []string{namespace}}
	stats, present, err := planSnapshotImport(t.Context(), target, manifest, index, false)
	if err != nil {
		t.Fatal(err)
	}
	if stats.unchanged != 1 || stats.conflicts != 1 || stats.newKeys != 1 || stats.expiredSinceValidation != 1 ||
		len(present[namespace]) != 2 || len(index[namespace]) != 4 {
		t.Fatalf("unexpected plan: stats=%+v present=%v index=%v", stats, present, index)
	}
}

func TestBatchedSnapshotRehearsal(t *testing.T) {
	addr := os.Getenv("KVSTORE_TEST_REHEARSAL_ADDR")
	if addr == "" {
		t.Skip("set KVSTORE_TEST_REHEARSAL_ADDR to a disposable rehearsal router")
	}
	const records = 512
	namespace := fmt.Sprintf("synthetic-import-throughput-%d", time.Now().UnixNano())
	var rows []*kv.SetRequest
	for i := range records {
		rows = append(rows, &kv.SetRequest{
			Namespace: namespace, Key: fmt.Sprintf("player-%06d", i),
			Value: bytes.Repeat([]byte{byte(i % 256)}, 1024),
		})
	}
	path := filepath.Join(t.TempDir(), "batch.kvsnapshot")
	if err := os.WriteFile(path, snapshotFixture(t, make(map[string]struct{}), rows...), 0600); err != nil {
		t.Fatal(err)
	}
	conn, err := connectTarget(addr, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := kv.NewKVStoreClient(conn)
	start := time.Now()
	var serial totals
	for _, row := range rows {
		copy := &kv.SetRequest{Namespace: namespace + "-serial", Key: row.Key, Value: row.Value}
		if err := compareRecord(t.Context(), client, copy, true, &serial); err != nil {
			t.Fatal(err)
		}
	}
	serialDuration := time.Since(start)
	start = time.Now()
	if err := runSnapshot(t.Context(), path, addr, true, false, false, "", "", ""); err != nil {
		t.Fatal(err)
	}
	batchDuration := time.Since(start)
	if err := runSnapshot(t.Context(), path, addr, false, true, false, "", "", ""); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d synthetic 1 KiB records: serial=%s batch=%s", records, serialDuration, batchDuration)
}
