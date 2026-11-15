package cluster

import (
	"testing"

	"github.com/hashicorp/raft"
)

func TestMeasuredRaftLogStoreAndCache(t *testing.T) {
	stats := &raftLogMetrics{}
	store := &measuredLogStore{inner: raft.NewInmemStore(), stats: stats}
	if store.IsMonotonic() {
		t.Fatal("memory store should not claim to be monotonic")
	}
	cache, err := raft.NewLogCache(32, store)
	if err != nil {
		t.Fatal(err)
	}
	entries := []*raft.Log{{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("abc")}, {Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("def")}}
	if err := cache.StoreLogs(entries); err != nil {
		t.Fatal(err)
	}
	var got raft.Log
	if err := cache.GetLog(2, &got); err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != "def" {
		t.Fatalf("wrong log %q", got.Data)
	}
	p := stats.Snapshot()
	if p.WriteCalls != 1 || p.StoredLogs != 2 || p.StoredBytes != 6 || p.WriteErrors != 0 {
		t.Fatalf("wrong write metrics: %+v", p)
	}
	// Cache reads should not hit the durable backend.
	if p.ReadMisses != 0 {
		t.Fatalf("expected cached read: %+v", p)
	}
	if err := cache.DeleteRange(1, 2); err != nil {
		t.Fatal(err)
	}
	if stats.Snapshot().DeleteCalls != 1 {
		t.Fatal("delete was not tracked")
	}
}
