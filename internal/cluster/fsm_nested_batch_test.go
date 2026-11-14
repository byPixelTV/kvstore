package cluster

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Multiple independent BatchWrite calls in one Raft log must keep separate
// atomicity boundaries while sharing ONE synchronous Pebble commit.
func TestNestedBatchAndPointWritePersistInOneRaftLog(t *testing.T) {
	path := t.TempDir()
	f, err := openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd := command{Operation: opMicrobatch, Commands: []command{
		{Operation: opBatch, Commands: []command{
			{Operation: opSet, Namespace: "n", Key: "same", Value: []byte("one")},
			{Operation: opDelete, Namespace: "n", Key: "same"},
			{Operation: opSet, Namespace: "n", Key: "same", Value: []byte("two")},
		}},
		{Operation: opSet, Namespace: "n", Key: "single", Value: []byte("one")},
		{Operation: opBatch, Commands: []command{
			{Operation: opSet, Namespace: "n", Key: "single", Value: []byte("two")},
			{Operation: opDelete, Namespace: "n", Key: "missing"},
		}},
	}}
	raw, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	res := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 1, Data: raw}).(applyResult)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if len(res.Revisions) != 3 || len(res.Errors) != 3 || len(res.BatchRevisions) != 3 {
		t.Fatalf("invalid nested response: %+v", res)
	}
	if len(res.BatchRevisions[0]) != 3 || res.BatchRevisions[0][0] != 1 || res.BatchRevisions[0][1] != 2 || res.BatchRevisions[0][2] != 3 {
		t.Fatalf("first nested batch revisions: %v", res.BatchRevisions[0])
	}
	if res.Revisions[1] != 4 || len(res.BatchRevisions[2]) != 2 || res.BatchRevisions[2][0] != 5 || res.BatchRevisions[2][1] != 0 {
		t.Fatalf("second nested batch revisions: %+v", res)
	}
	perf := f.Performance()
	if perf.CommitBatches != 1 || perf.LogicalWrites != 5 || perf.CommittedLogs != 1 {
		t.Fatalf("expected one durable batch for five mutations: %+v", perf)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for key, value := range map[string]string{"same": "two", "single": "two"} {
		got, found, err := f.Get(context.Background(), "n", key)
		if err != nil || !found || string(got.Value) != value {
			t.Fatalf("%s: %+v %t %v", key, got, found, err)
		}
	}
	page, err := f.ReadChanges(context.Background(), 0, 10)
	if err != nil || page.Head != 5 || len(page.Events) != 5 {
		t.Fatalf("changefeed: %+v %v", page, err)
	}
}

// Fence must reject the WHOLE client BatchWrite, not adjacent point writes or
// another independent BatchWrite that shares its physical Raft append.
func TestNestedBatchFenceRejectsOnlyAffectedClient(t *testing.T) {
	f, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ns := "migration"
	fenced := "sealed"
	safe := "safe"
	for sharding.Slot(ns, fenced) == sharding.Slot(ns, safe) {
		safe += "-another"
	}
	fence, _ := json.Marshal(command{Operation: opFence, Namespace: ns, Slot: uint32(sharding.Slot(ns, fenced)), Sealed: true})
	if res := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 1, Data: fence}).(applyResult); res.Err != nil {
		t.Fatal(res.Err)
	}
	cmd := command{Operation: opMicrobatch, Commands: []command{
		{Operation: opSet, Namespace: ns, Key: safe, Value: []byte("yes")},
		{Operation: opBatch, Commands: []command{
			{Operation: opSet, Namespace: ns, Key: safe, Value: []byte("bad")},
			{Operation: opSet, Namespace: ns, Key: fenced, Value: []byte("bad")},
		}},
		{Operation: opBatch, Commands: []command{
			{Operation: opSet, Namespace: ns, Key: safe, Value: []byte("done")},
		}},
	}}
	raw, _ := json.Marshal(cmd)
	res := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 2, Data: raw}).(applyResult)
	if res.Err != nil || len(res.Errors) != 3 || res.Errors[0] != nil || status.Code(res.Errors[1]) != codes.FailedPrecondition || res.Errors[2] != nil {
		t.Fatalf("fence incorrectly rejected adjacent writes: %+v", res)
	}
	if res.Revisions[0] != 1 || len(res.BatchRevisions[2]) != 1 || res.BatchRevisions[2][0] != 2 {
		t.Fatalf("revisions do not preserve accepted writes: %+v", res)
	}
	got, exists, err := f.Get(context.Background(), ns, safe)
	if err != nil || !exists || string(got.Value) != "done" {
		t.Fatalf("surviving key: %+v %t %v", got, exists, err)
	}
	if _, exists, err := f.Get(context.Background(), ns, fenced); err != nil || exists {
		t.Fatalf("fenced key visible %t %v", exists, err)
	}
}
