package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestPersistenceAcrossReopen(t *testing.T) {
	path := t.TempDir()
	ctx := context.Background()

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Set(ctx, "players", "123", []byte("Hello"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Set(ctx, "settings", "123", []byte("World"))
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 2 {
		t.Fatalf("unexpected revisions: %d, %d", first, second)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range []struct{ namespace, want string }{
		{"players", "Hello"},
		{"settings", "World"},
	} {
		record, found, err := s.Get(ctx, tc.namespace, "123")
		if err != nil || !found || string(record.Value) != tc.want {
			t.Fatalf("reopen Get %q: record=%+v found=%v err=%v", tc.namespace, record, found, err)
		}
	}

	third, err := s.Set(ctx, "players", "456", []byte("new"))
	if err != nil || third != 3 {
		t.Fatalf("revision after reopen: got %d, err=%v; want 3", third, err)
	}
}

func TestNamespacesAndBinaryIsolation(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	value := []byte{0x00, 0x01, 0xff}
	if _, err := s.Set(ctx, "a/b", "c", value); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Set(ctx, "a", "b/c", []byte("different")); err != nil {
		t.Fatal(err)
	}
	value[0] = 99 // Must not modify persistent data.

	record, found, err := s.Get(ctx, "a/b", "c")
	if err != nil || !found || !bytes.Equal(record.Value, []byte{0, 1, 255}) {
		t.Fatalf("unexpected value: record=%+v found=%v err=%v", record, found, err)
	}
	record.Value[0] = 99
	again, _, err := s.Get(ctx, "a/b", "c")
	if err != nil || !bytes.Equal(again.Value, []byte{0, 1, 255}) {
		t.Fatalf("external mutation affected value: %+v, err=%v", again, err)
	}
	other, found, err := s.Get(ctx, "a", "b/c")
	if err != nil || !found || string(other.Value) != "different" {
		t.Fatalf("namespace/key collision: record=%+v found=%v err=%v", other, found, err)
	}
}

func TestDeleteRevisionAndReopen(t *testing.T) {
	path := t.TempDir()
	ctx := context.Background()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Set(ctx, "players", "123", []byte("first"))
	deleted, err := s.Delete(ctx, "players", "123")
	if err != nil || deleted != 2 {
		t.Fatalf("Delete: revision=%d err=%v", deleted, err)
	}
	again, err := s.Delete(ctx, "players", "123")
	if err != nil || again != 0 {
		t.Fatalf("Delete missing: revision=%d err=%v", again, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, found, err := s.Get(ctx, "players", "123")
	if err != nil || found {
		t.Fatalf("deleted record reappeared: found=%v err=%v", found, err)
	}
	revision, err := s.Set(ctx, "players", "456", []byte("ok"))
	if err != nil || revision != 3 {
		t.Fatalf("revision after delete + reopen: %d, err=%v", revision, err)
	}
}

func TestCancelledContext(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Set(ctx, "players", "123", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Set: expected canceled, got %v", err)
	}
	if _, _, err := s.Get(ctx, "players", "123"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get: expected canceled, got %v", err)
	}
	if _, err := s.Delete(ctx, "players", "123"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete: expected canceled, got %v", err)
	}
}

func TestConcurrentWritesHaveUniqueRevisions(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const count = 40
	var wg sync.WaitGroup
	revisions := make(chan uint64, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			revision, err := s.Set(context.Background(), "players", fmt.Sprintf("%d", i), []byte("value"))
			if err != nil {
				t.Errorf("Set: %v", err)
				return
			}
			revisions <- revision
		}(i)
	}
	wg.Wait()
	close(revisions)
	seen := make(map[uint64]bool)
	for rev := range revisions {
		if rev == 0 || seen[rev] {
			t.Errorf("invalid/duplicate revision: %d", rev)
		}
		seen[rev] = true
	}
	if len(seen) != count {
		t.Fatalf("expected %d revisions, got %d", count, len(seen))
	}
}
