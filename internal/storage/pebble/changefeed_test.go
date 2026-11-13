package pebblestore

import (
	"context"
	"errors"
	"testing"

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/cockroachdb/pebble/v2"
)

func TestChangefeedPersistsReplaysAndScans(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := s.Set(ctx, "players", "p1", []byte("Alice"))
	if err != nil || rev != 1 {
		t.Fatalf("set: %d %v", rev, err)
	}
	_, err = s.Set(ctx, "settings", "s1", []byte("on"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Delete(ctx, "players", "p1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	page, err := s.ReadChanges(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if page.Head != 3 || page.Oldest != 1 || len(page.Events) != 3 {
		t.Fatalf("page=%+v", page)
	}
	if string(page.Events[0].Value) != "Alice" || !page.Events[2].Deleted || page.Events[2].Revision != 3 {
		t.Fatalf("events=%+v", page.Events)
	}
	page, err = s.ReadChanges(ctx, 1, 1)
	if err != nil || len(page.Events) != 1 || page.Events[0].Revision != 2 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	items := []storage.ScanItem{}
	err = s.ScanNamespace(ctx, "settings", func(item storage.ScanItem) error { items = append(items, item); return nil })
	if err != nil || len(items) != 2 || !items[0].Checkpoint || items[0].SnapshotRevision != 3 || items[1].Key != "s1" || string(items[1].Value) != "on" {
		t.Fatalf("scan=%+v err=%v", items, err)
	}
}

func TestWatchNotifierAndNoopDelete(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	signal := s.Notify()
	rev, err := s.Delete(ctx, "n", "missing")
	if err != nil || rev != 0 {
		t.Fatalf("delete: %d %v", rev, err)
	}
	select {
	case <-signal:
		t.Fatal("no-op delete notified")
	default:
	}
	_, err = s.Set(ctx, "n", "k", []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-signal:
	default:
		t.Fatal("write did not notify")
	}
	if _, err = s.ReadChanges(ctx, 0, 0); err == nil {
		t.Fatal("zero page limit accepted")
	}
	if _, err = s.ReadChanges(ctx, 0, 10); errors.Is(err, changefeed.ErrHistoryUnavailable) {
		t.Fatal("new history unexpectedly missing")
	}
}

func TestLegacyPebbleRequiresResync(t *testing.T) {
	path := t.TempDir()
	db, err := pebble.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(revisionKey, changefeed.EncodedFloor(5), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	head, floor, err := s.Head(context.Background())
	if err != nil || head != 5 || floor != 6 {
		t.Fatalf("head=%d floor=%d err=%v", head, floor, err)
	}
	_, err = s.ReadChanges(context.Background(), 0, 100)
	if !errors.Is(err, changefeed.ErrHistoryUnavailable) {
		t.Fatalf("expected resync, got %v", err)
	}
	_, err = s.Set(context.Background(), "n", "k", []byte("new"))
	if err != nil {
		t.Fatal(err)
	}
	page, err := s.ReadChanges(context.Background(), 5, 100)
	if err != nil || len(page.Events) != 1 || page.Events[0].Revision != 6 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}
