package cluster

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
)

func TestRaftFSMBatchMultipleMutationsAndReopen(t *testing.T) {
	path := t.TempDir()
	f, err := openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	cmds := []command{
		{Operation: opSet, Namespace: "n", Key: "same", Value: []byte("a")},
		{Operation: opDelete, Namespace: "n", Key: "same"},
		{Operation: opDelete, Namespace: "n", Key: "same"},
		{Operation: opSet, Namespace: "n", Key: "same", Value: []byte("b")},
		{Operation: opPeer, Peer: &peerInfo{ID: "node1", RaftAddr: "n:7000", GRPCAddr: "n:50051", AdminAddr: "n:7001"}},
	}
	logs := make([]*raft.Log, 0, len(cmds)+1)
	for i, cmd := range cmds {
		raw, err := json.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, &raft.Log{Type: raft.LogCommand, Index: uint64(i + 1), Data: raw})
	}
	// Configuration entries can be present in BatchingFSM.ApplyBatch.
	logs = append(logs, &raft.Log{Type: raft.LogConfiguration, Index: 6})
	results := f.ApplyBatch(logs)
	if len(results) != len(logs) {
		t.Fatalf("results len %d", len(results))
	}
	for i, want := range []uint64{1, 2, 0, 3, 0, 0} {
		got, ok := results[i].(applyResult)
		if !ok || got.Err != nil || got.Revision != want {
			t.Fatalf("result %d = %+v want rev %d", i, results[i], want)
		}
	}
	if f.applied != 6 || f.revision != 3 {
		t.Fatalf("counters applied=%d rev=%d", f.applied, f.revision)
	}
	got, found, err := f.Get(context.Background(), "n", "same")
	if err != nil || !found || string(got.Value) != "b" || got.Revision != 3 {
		t.Fatalf("get %+v,found=%v err=%v", got, found, err)
	}
	page, err := f.ReadChanges(context.Background(), 0, 10)
	if err != nil || page.Head != 3 || len(page.Events) != 3 || !page.Events[1].Deleted {
		t.Fatalf("page %+v err=%v", page, err)
	}
	peer, exists, err := f.Peer("node1")
	if err != nil || !exists || peer.GRPCAddr != "n:50051" {
		t.Fatalf("peer %+v exists=%v err=%v", peer, exists, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.applied != 6 || f.revision != 3 {
		t.Fatalf("recovered counters %d %d", f.applied, f.revision)
	}
	replay := f.ApplyBatch(logs)
	for i, r := range replay {
		got, ok := r.(applyResult)
		if !ok || got.Err != nil || got.Revision != 0 {
			t.Fatalf("replay result %d: %+v", i, r)
		}
	}
	got, found, err = f.Get(context.Background(), "n", "same")
	if err != nil || !found || string(got.Value) != "b" {
		t.Fatalf("replay state %+v,%v,%v", got, found, err)
	}
}

func TestRaftFSMBatchRejectsUnknownWithoutPartialWrite(t *testing.T) {
	f, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	a, _ := json.Marshal(command{Operation: opSet, Namespace: "n", Key: "k", Value: []byte("value")})
	b, _ := json.Marshal(command{Operation: "not-supported"})
	results := f.ApplyBatch([]*raft.Log{{Type: raft.LogCommand, Index: 1, Data: a}, {Type: raft.LogCommand, Index: 2, Data: b}})
	for _, r := range results {
		if r.(applyResult).Err == nil {
			t.Fatal("expected error for entire atomic group")
		}
	}
	if f.applied != 0 || f.revision != 0 {
		t.Fatalf("batch partly applied: %d, %d", f.applied, f.revision)
	}
}
