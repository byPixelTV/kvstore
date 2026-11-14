package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWriteGroupSameKeyOrderDurabilityAndFeed(t *testing.T) {
	path := t.TempDir()
	s, err := OpenWithOptions(path, Options{BatchWindow: 3 * time.Millisecond, BatchMax: 64})
	if err != nil {
		t.Fatal(err)
	}
	mk := func(kind writeKind, value string) writeRequest {
		return writeRequest{ctx: context.Background(), kind: kind, namespace: "players", key: "same", value: []byte(value), reply: make(chan writeResponse, 1)}
	}
	ops := []writeRequest{
		mk(setKind, "a"), mk(deleteKind, ""), mk(deleteKind, ""),
		mk(setKind, "b"), mk(setKind, "c"),
	}
	s.applyGroup(ops) // deterministic ordering in one durable batch
	for i, want := range []uint64{1, 2, 0, 3, 4} {
		got := <-ops[i].reply
		if got.err != nil || got.revision != want {
			t.Fatalf("op %d: %+v, want rev %d", i, got, want)
		}
	}
	got, found, err := s.Get(context.Background(), "players", "same")
	if err != nil || !found || got.Revision != 4 || string(got.Value) != "c" {
		t.Fatalf("get: %+v, %v, %v", got, found, err)
	}
	page, err := s.ReadChanges(context.Background(), 0, 10)
	if err != nil || page.Head != 4 || len(page.Events) != 4 || !page.Events[1].Deleted {
		t.Fatalf("feed: %+v, err=%v", page, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, found, err = s.Get(context.Background(), "players", "same")
	if err != nil || !found || got.Revision != 4 || string(got.Value) != "c" {
		t.Fatalf("reopened: %+v, %v, %v", got, found, err)
	}
}

func TestManyConcurrentGroupedWritesAndClose(t *testing.T) {
	path := t.TempDir()
	s, err := OpenWithOptions(path, Options{BatchWindow: 2 * time.Millisecond, BatchMax: 32})
	if err != nil {
		t.Fatal(err)
	}
	const count = 160
	start := make(chan struct{})
	results := make(chan writeResponse, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rev, err := s.Set(context.Background(), "n", fmt.Sprintf("key-%d", i), []byte("value"))
			results <- writeResponse{revision: rev, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	seen := make(map[uint64]bool, count)
	for res := range results {
		if res.err != nil || res.revision == 0 || seen[res.revision] {
			t.Fatalf("bad result %+v", res)
		}
		seen[res.revision] = true
	}
	if len(seen) != count {
		t.Fatalf("got %d unique revisions", len(seen))
	}
	page, err := s.ReadChanges(context.Background(), 0, count+1)
	if err != nil || len(page.Events) != count || page.Head != count {
		t.Fatalf("feed length=%d head=%d err=%v", len(page.Events), page.Head, err)
	}
	for i, event := range page.Events {
		if event.Revision != uint64(i+1) {
			t.Fatalf("feed[%d] rev %d", i, event.Revision)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Set(context.Background(), "n", "late", nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed: %v", err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	head, _, err := s.Head(context.Background())
	if err != nil || head != count {
		t.Fatalf("reopened head=%d err=%v", head, err)
	}
}

func TestBatchedWriteCancellationBeforeEnqueue(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Set(ctx, "n", "k", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	head, _, _ := s.Head(context.Background())
	if head != 0 {
		t.Fatalf("cancelled write mutated state: %d", head)
	}
	page, err := s.ReadChanges(context.Background(), 0, 10)
	if err != nil || len(page.Events) != 0 || page.Oldest != 1 {
		t.Fatalf("feed %+v %v", page, err)
	}
}
