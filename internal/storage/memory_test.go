package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestMemoryStorageSetAndGet(t *testing.T) {
	store := New()
	ctx := context.Background()

	writtenRevision, err := store.Set(ctx, "players", "123", []byte("Pixel"))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	record, found, err := store.Get(ctx, "players", "123")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("Expected key to exist")
	}
	if !bytes.Equal(record.Value, []byte("Pixel")) {
		t.Fatalf("Expected Pixel, got %q", record.Value)
	}
	if record.Revision != writtenRevision || record.Revision == 0 {
		t.Fatalf("Unexpected revision: Set=%d, Get=%d", writtenRevision, record.Revision)
	}
}

func TestMemoryStorageMissingKey(t *testing.T) {
	store := New()
	record, found, err := store.Get(context.Background(), "players", "missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found || record.Revision != 0 || record.Value != nil {
		t.Fatalf("Expected no record, got %+v (found=%v)", record, found)
	}
}

func TestMemoryStorageNamespaceIsolation(t *testing.T) {
	store := New()
	ctx := context.Background()

	if _, err := store.Set(ctx, "players", "123", []byte("profile")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Set(ctx, "settings", "123", []byte("settings")); err != nil {
		t.Fatal(err)
	}

	for namespace, expected := range map[string]string{
		"players":  "profile",
		"settings": "settings",
	} {
		record, found, err := store.Get(ctx, namespace, "123")
		if err != nil || !found || string(record.Value) != expected {
			t.Fatalf("namespace %q: record=%+v found=%v err=%v", namespace, record, found, err)
		}
	}
}

func TestMemoryStorageOverwriteAndRevisions(t *testing.T) {
	store := New()
	ctx := context.Background()

	first, err := store.Set(ctx, "players", "123", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Set(ctx, "players", "123", []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if second != first+1 {
		t.Fatalf("Expected revision %d, got %d", first+1, second)
	}

	record, found, err := store.Get(ctx, "players", "123")
	if err != nil || !found || string(record.Value) != "second" || record.Revision != second {
		t.Fatalf("Unexpected record: %+v, found=%v, err=%v", record, found, err)
	}
}

func TestMemoryStorageDelete(t *testing.T) {
	store := New()
	ctx := context.Background()

	setRevision, err := store.Set(ctx, "players", "123", []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	deleteRevision, err := store.Delete(ctx, "players", "123")
	if err != nil {
		t.Fatal(err)
	}
	if deleteRevision != setRevision+1 {
		t.Fatalf("Expected revision %d, got %d", setRevision+1, deleteRevision)
	}
	_, found, err := store.Get(ctx, "players", "123")
	if err != nil || found {
		t.Fatalf("Expected deleted key, found=%v, err=%v", found, err)
	}

	// Deleting a missing key is a no-op: no new revision.
	missingRevision, err := store.Delete(ctx, "players", "123")
	if err != nil || missingRevision != 0 {
		t.Fatalf("Expected revision 0, got %d (err=%v)", missingRevision, err)
	}
	if store.revision != deleteRevision {
		t.Fatalf("No-op delete changed revision: %d", store.revision)
	}
}

func TestMemoryStorageByteSliceIsolation(t *testing.T) {
	store := New()
	ctx := context.Background()
	value := []byte("hello")

	if _, err := store.Set(ctx, "players", "123", value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'

	record, found, err := store.Get(ctx, "players", "123")
	if err != nil || !found || string(record.Value) != "hello" {
		t.Fatalf("Set did not clone bytes: %+v, found=%v, err=%v", record, found, err)
	}
	record.Value[0] = 'Y'

	again, _, err := store.Get(ctx, "players", "123")
	if err != nil || string(again.Value) != "hello" {
		t.Fatalf("Get did not clone bytes: %+v, err=%v", again, err)
	}
}

func TestMemoryStorageCancelledContext(t *testing.T) {
	store := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.Set(ctx, "players", "123", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Set: expected context.Canceled, got %v", err)
	}
	if _, _, err := store.Get(ctx, "players", "123"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get: expected context.Canceled, got %v", err)
	}
	if _, err := store.Delete(ctx, "players", "123"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete: expected context.Canceled, got %v", err)
	}
	if store.revision != 0 || len(store.data) != 0 {
		t.Fatal("Cancelled operations modified storage")
	}
}

func TestMemoryStorageConcurrentAccess(t *testing.T) {
	store := New()
	ctx := context.Background()
	const goroutines = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			expected := []byte(fmt.Sprintf("value-%d", id))

			if _, err := store.Set(ctx, "players", key, expected); err != nil {
				t.Errorf("Set %s: %v", key, err)
				return
			}
			record, found, err := store.Get(ctx, "players", key)
			if err != nil || !found || !bytes.Equal(record.Value, expected) {
				t.Errorf("Get %s: record=%+v found=%v err=%v", key, record, found, err)
				return
			}
			if _, err := store.Delete(ctx, "players", key); err != nil {
				t.Errorf("Delete %s: %v", key, err)
			}
		}(i)
	}
	wg.Wait()

	if len(store.data) != 0 {
		t.Fatalf("Expected empty storage, got %d entries", len(store.data))
	}
	if store.revision != 2*goroutines {
		t.Fatalf("Expected revision %d, got %d", 2*goroutines, store.revision)
	}
}
