package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/hashicorp/raft"
)

func TestReplicatedSlotFencePersistsAndRejectsLateWrites(t *testing.T) {
	path := t.TempDir()
	f, err := openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	ns, key := "players", "player:42"
	slot := uint32(sharding.Slot(ns, key))
	raftEntry(t, f, 1, command{Operation: opSet, Namespace: ns, Key: key, Value: []byte("before")})
	raftEntry(t, f, 2, command{Operation: opFence, Namespace: ns, Slot: slot, Sealed: true})
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	late, _ := json.Marshal(command{Operation: opSet, Namespace: ns, Key: key, Value: []byte("late")})
	response := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 3, Data: late}).(applyResult)
	if response.Err == nil {
		t.Fatal("late write accepted after replicated fence")
	}
	if f.Failure() != nil {
		t.Fatalf("soft rejection permanently poisoned Raft FSM: %v", f.Failure())
	}
	got, found, err := f.Get(context.Background(), ns, key)
	if err != nil || !found || string(got.Value) != "before" {
		t.Fatalf("stale value changed %v %v %v", got, found, err)
	}
	if f.applied != 3 || f.revision != 1 {
		t.Fatalf("fenced write advanced revision: applied=%d rev=%d", f.applied, f.revision)
	}

	// An entire mixed-slot batch must fail atomically, not leak unrelated writes.
	batch, _ := json.Marshal(command{Operation: opBatch, Commands: []command{
		{Operation: opSet, Namespace: ns, Key: key, Value: []byte("bad")},
		{Operation: opSet, Namespace: ns, Key: "unrelated", Value: []byte("bad")},
	}})
	response = f.Apply(&raft.Log{Type: raft.LogCommand, Index: 4, Data: batch}).(applyResult)
	if response.Err == nil {
		t.Fatal("partly applied fenced batch")
	}
	if _, found, _ := f.Get(context.Background(), ns, "unrelated"); found {
		t.Fatal("batch leaked unrelated key")
	}

	raftEntry(t, f, 5, command{Operation: opFence, Namespace: ns, Slot: slot, Sealed: false})
	raftEntry(t, f, 6, command{Operation: opSet, Namespace: ns, Key: key, Value: []byte("after")})
	got, found, err = f.Get(context.Background(), ns, key)
	if err != nil || !found || string(got.Value) != "after" {
		t.Fatalf("unseal failed: %v %v %v", got, found, err)
	}
}

func TestSlotFenceRestoredFromRaftSnapshot(t *testing.T) {
	// Fence records live in the Pebble snapshot keyspace; the existing
	// Raft snapshot serializer includes them without changing its format.
	f, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	raftEntry(t, f, 1, command{Operation: opFence, Namespace: "n", Slot: 13, Sealed: true})
	if !f.sealed[sealID{namespace: "n", slot: 13}] {
		t.Fatal("fence not visible after apply")
	}
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	snapshot.Release()
	if err := f.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if !f.sealed[sealID{namespace: "n", slot: 13}] {
		t.Fatal("Raft snapshot lost sealed slot")
	}
}
