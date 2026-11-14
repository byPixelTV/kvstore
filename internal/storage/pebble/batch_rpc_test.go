package pebblestore

import (
	"context"
	"testing"

	"github.com/byPixelTV/kvstore/internal/storage"
)

func TestAtomicBatchWriteRevisionOrderAndRestart(t *testing.T) {
	path := t.TempDir()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	revisions, err := store.BatchWrite(ctx, []storage.WriteOperation{
		{Namespace: "players", Key: "id", Value: []byte("first")},
		{Namespace: "players", Key: "id", Delete: true},
		{Namespace: "players", Key: "id", Delete: true},
		{Namespace: "players", Key: "id", Value: []byte("second")},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{1, 2, 0, 3}
	if len(revisions) != len(want) {
		t.Fatalf("revisions %v", revisions)
	}
	for i, w := range want {
		if revisions[i] != w {
			t.Fatalf("revisions[%d]=%d, want %d", i, revisions[i], w)
		}
	}
	got, found, err := store.Get(ctx, "players", "id")
	if err != nil || !found || string(got.Value) != "second" || got.Revision != 3 {
		t.Fatalf("unexpected read: %+v %v %v", got, found, err)
	}
	page, err := store.ReadChanges(ctx, 0, 16)
	if err != nil || page.Head != 3 || len(page.Events) != 3 {
		t.Fatalf("changefeed: %+v %v", page, err)
	}
	if !page.Events[1].Deleted {
		t.Fatal("delete event missing")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, found, err = store.Get(ctx, "players", "id")
	if err != nil || !found || got.Revision != 3 || string(got.Value) != "second" {
		t.Fatalf("reopen: %+v %v %v", got, found, err)
	}
}

func TestBatchWriterClosedRejectsRequest(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BatchWrite(context.Background(), []storage.WriteOperation{{Namespace: "n", Key: "k"}}); err != ErrStoreClosed {
		t.Fatalf("expected closed error: %v", err)
	}
}
