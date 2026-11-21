package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/clienttls"
	"google.golang.org/grpc"
)

func snapshotFixture(t *testing.T, namespaces map[string]struct{}, rows ...*kv.SetRequest) []byte {
	t.Helper()
	var out bytes.Buffer
	_, err := writeSnapshot(t.Context(), &out, namespaces, func(stats *totals, emit func(*kv.SetRequest) error) error {
		stats.skippedExpiring = 1
		for _, row := range rows {
			stats.records++
			if err := emit(row); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestSnapshotRoundTrip(t *testing.T) {
	row := &kv.SetRequest{Namespace: "inventory", Key: "player", Value: []byte{0, 0xff, 'a'}}
	data := snapshotFixture(t, map[string]struct{}{"cooldown": {}}, row)
	manifest, index, err := loadSnapshot(t.Context(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Records != 1 || manifest.SkippedExpiring != 1 || strings.Join(manifest.Namespaces, ",") != "cooldown,inventory" ||
		index["inventory"]["player"] != expectRecord(row.Value, 0) {
		t.Fatalf("unexpected manifest and index: %+v %+v", manifest, index)
	}
	legacy := bytes.Replace(data, []byte(snapshotHeader), []byte(legacySnapshotHeader), 1)
	if _, _, err := loadSnapshot(t.Context(), bytes.NewReader(legacy)); err != nil {
		t.Fatalf("previous snapshots must remain readable: %v", err)
	}
	for name, corrupt := range map[string][]byte{
		"truncated":       data[:len(data)-20],
		"altered payload": bytes.Replace(data, []byte("AP9h"), []byte("AP9i"), 1),
		"altered summary": bytes.Replace(data, []byte(`"skipped_expiring":1`), []byte(`"skipped_expiring":2`), 1),
		"duplicate":       snapshotFixture(t, make(map[string]struct{}), row, row),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := loadSnapshot(t.Context(), bytes.NewReader(corrupt)); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}

func TestMongoDeadlinePreservesAbsoluteExpiry(t *testing.T) {
	earlier := time.UnixMilli(750123)
	later := int64(800456)
	deadline, set := mongoDeadline(document{ExpiresAt: &later, ExpiresAtDate: &earlier})
	if !set || deadline != earlier.UnixMilli() {
		t.Fatalf("expected original earlier deadline, got %v (%t)", deadline, set)
	}
	row := &kv.SetRequest{Namespace: "cooldowns", Key: "player", ExpiresAtUnixMs: deadline}
	data := snapshotFixture(t, make(map[string]struct{}), row)
	manifest, index, err := loadSnapshot(t.Context(), bytes.NewReader(data))
	if err != nil || manifest.Expiring != 1 || index["cooldowns"]["player"].expiresAtUnixMS != deadline {
		t.Fatalf("TTL missing from snapshot: %+v %v", manifest, err)
	}
	legacy := bytes.Replace(data, []byte(snapshotHeader), []byte(legacySnapshotHeader), 1)
	if _, _, err := loadSnapshot(t.Context(), bytes.NewReader(legacy)); err == nil {
		t.Fatal("a v1 snapshot must not accept a TTL record")
	}
}

func TestSnapshotExportKeepsExistingFilesAndCleansUpFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.kvsnapshot")
	stop := errors.New("source read failed")
	partial := func(stats *totals, emit func(*kv.SetRequest) error) error {
		stats.records++
		if err := emit(&kv.SetRequest{Namespace: "players", Key: "test"}); err != nil {
			return err
		}
		return stop
	}
	if err := exportSnapshot(t.Context(), path, nil, partial); !errors.Is(err, stop) {
		t.Fatalf("expected source failure, got %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("failed export left an output file: %v", err)
	}
	if err := os.WriteFile(path, []byte("existing snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := exportSnapshot(t.Context(), path, nil, partial); !os.IsExist(err) {
		t.Fatalf("expected existing-file failure, got %v", err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "existing snapshot" {
		t.Fatalf("existing snapshot modified: %q (%v)", data, err)
	}
}

type scanTarget struct {
	kv.KVStoreClient
	items []*kv.ScanResponse
}

func (s scanTarget) Scan(_ context.Context, _ *kv.ScanRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[kv.ScanResponse], error) {
	return &scanResponses{items: s.items}, nil
}

type scanResponses struct {
	grpc.ServerStreamingClient[kv.ScanResponse]
	items []*kv.ScanResponse
}

func (s *scanResponses) Recv() (*kv.ScanResponse, error) {
	if len(s.items) == 0 {
		return nil, io.EOF
	}
	item := s.items[0]
	s.items = s.items[1:]
	return item, nil
}

func TestSnapshotVerificationFindsMissingChangedAndExtra(t *testing.T) {
	target := scanTarget{items: []*kv.ScanResponse{
		{Key: "matching", Value: []byte("same")},
		{Key: "changed", Value: []byte("updated")},
		{Key: "extra", Value: []byte("other")},
		{Checkpoint: true, Cursor: "vector-checkpoint"},
	}}
	expected := map[string]expectedRecord{
		"matching": expectRecord([]byte("same"), 0),
		"changed":  expectRecord([]byte("original"), 0),
		"missing":  expectRecord([]byte("absent"), 0),
	}
	var stats verificationTotals
	if err := verifyNamespace(t.Context(), target, "players", expected, false, &stats); err != nil {
		t.Fatal(err)
	}
	if stats != (verificationTotals{matching: 1, missing: 1, changed: 1, extra: 1}) {
		t.Fatalf("unexpected verification result: %+v", stats)
	}

	target.items = target.items[:len(target.items)-1]
	if err := verifyNamespace(t.Context(), target, "players", nil, false, &stats); err == nil {
		t.Fatal("incomplete router scan accepted")
	}
}

// Run only against the isolated rehearsal-check cluster.
func TestSnapshotRehearsalRouter(t *testing.T) {
	addr := os.Getenv("KVSTORE_TEST_REHEARSAL_ADDR")
	if addr == "" {
		t.Skip("set KVSTORE_TEST_REHEARSAL_ADDR to the disposable rehearsal-check router")
	}
	namespace := fmt.Sprintf("synthetic-import-rehearsal-%d", time.Now().UnixNano())
	row := &kv.SetRequest{Namespace: namespace, Key: "inventory", Value: []byte{0, 0xff, 'a'}}
	fixture := snapshotFixture(t, map[string]struct{}{"synthetic-empty-namespace": {}}, row)
	path := filepath.Join(t.TempDir(), "source.kvsnapshot")
	if err := os.WriteFile(path, fixture, 0600); err != nil {
		t.Fatal(err)
	}
	if err := runSnapshot(t.Context(), path, addr, false, true, false, clienttls.Options{}); err == nil || err.Error() != "target differs from snapshot" {
		t.Fatalf("expected missing-record failure before apply, got %v", err)
	}
	for range 2 {
		if err := runSnapshot(t.Context(), path, addr, true, false, false, clienttls.Options{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := runSnapshot(t.Context(), path, addr, false, true, false, clienttls.Options{}); err != nil {
		t.Fatal(err)
	}
	conn, err := connectTarget(addr, clienttls.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := kv.NewKVStoreClient(conn)
	if _, err := client.Set(t.Context(), &kv.SetRequest{Namespace: namespace, Key: "extra", Value: []byte("extra")}); err != nil {
		t.Fatal(err)
	}
	if err := runSnapshot(t.Context(), path, addr, false, true, false, clienttls.Options{}); err == nil || err.Error() != "target differs from snapshot" {
		t.Fatalf("expected extra-record failure, got %v", err)
	}
}

func TestSnapshotRehearsalTTL(t *testing.T) {
	addr := os.Getenv("KVSTORE_TEST_REHEARSAL_ADDR")
	if addr == "" {
		t.Skip("set KVSTORE_TEST_REHEARSAL_ADDR to a disposable TTL-capable rehearsal router")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	namespace := fmt.Sprintf("synthetic-cooldown-%d", time.Now().UnixNano())
	deadline := time.Now().Add(7 * time.Second).UnixMilli()
	row := &kv.SetRequest{Namespace: namespace, Key: "cooldown", Value: []byte("active"), ExpiresAtUnixMs: deadline}
	file := filepath.Join(t.TempDir(), "ttl.kvsnapshot")
	if err := os.WriteFile(file, snapshotFixture(t, make(map[string]struct{}), row), 0600); err != nil {
		t.Fatal(err)
	}
	conn, err := connectTarget(addr, clienttls.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := kv.NewKVStoreClient(conn)
	head, err := client.Head(ctx, &kv.HeadRequest{UseVectorCursor: true})
	if err != nil || !head.GetSupportsExpiration() {
		t.Fatalf("target lacks TTL support: %v", err)
	}
	stream, err := client.Watch(ctx, &kv.WatchRequest{Namespace: namespace, Cursor: head.GetCursor(), UseVectorCursor: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := runSnapshot(ctx, file, addr, true, false, false, clienttls.Options{}); err != nil {
		t.Fatal(err)
	}
	request := &kv.GetRequest{Namespace: namespace, Key: row.Key}
	reply, err := client.Get(ctx, request)
	if err != nil || !reply.GetFound() || reply.GetExpiresAtUnixMs() != deadline {
		t.Fatalf("TTL did not survive import: %+v %v", reply, err)
	}
	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if !event.GetCheckpoint() {
			if event.GetKey() != row.Key || event.GetExpiresAtUnixMs() != deadline {
				t.Fatalf("TTL lost from watch: %+v", event)
			}
			break
		}
	}
	if err := runSnapshot(ctx, file, addr, false, true, false, clienttls.Options{}); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Until(time.UnixMilli(deadline).Add(250 * time.Millisecond)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	reply, err = client.Get(ctx, request)
	if err != nil || reply.GetFound() {
		t.Fatalf("value still visible after original deadline: %+v %v", reply, err)
	}
	if err := runSnapshot(ctx, file, addr, false, true, false, clienttls.Options{}); err != nil {
		t.Fatal(err)
	}
}
