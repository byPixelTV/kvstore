package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func raftEntry(t *testing.T, f *FSM, index uint64, cmd command) applyResult {
	t.Helper()
	raw, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := f.Apply(&raft.Log{Index: index, Data: raw}).(applyResult)
	if !ok {
		t.Fatal("unexpected FSM result")
	}
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	return result
}

func TestFSMPersistsAppliedIndexAndRevision(t *testing.T) {
	path := t.TempDir()
	f, err := openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	if rev := raftEntry(t, f, 1, command{Operation: opSet, Namespace: "players", Key: "1", Value: []byte("hello")}).Revision; rev != 1 {
		t.Fatalf("revision=%d", rev)
	}
	if rev := raftEntry(t, f, 2, command{Operation: opDelete, Namespace: "players", Key: "missing"}).Revision; rev != 0 {
		t.Fatalf("revision=%d", rev)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.applied != 2 || f.revision != 1 {
		t.Fatalf("recovered state applied=%d revision=%d", f.applied, f.revision)
	}
	// Replaying an already committed index must NOT duplicate the write.
	if rev := raftEntry(t, f, 1, command{Operation: opSet, Namespace: "players", Key: "1", Value: []byte("wrong")}).Revision; rev != 0 {
		t.Fatalf("replay revision=%d", rev)
	}
	got, found, err := f.Get(context.Background(), "players", "1")
	if err != nil || !found || string(got.Value) != "hello" {
		t.Fatalf("duplicate apply damaged data: %+v found=%v err=%v", got, found, err)
	}
	if rev := raftEntry(t, f, 3, command{Operation: opDelete, Namespace: "players", Key: "1"}).Revision; rev != 2 {
		t.Fatalf("delete revision=%d", rev)
	}
}

type memorySink struct {
	bytes.Buffer
	closed   bool
	canceled bool
}

func (s *memorySink) ID() string    { return "test" }
func (s *memorySink) Cancel() error { s.canceled = true; return nil }
func (s *memorySink) Close() error  { s.closed = true; return nil }

func TestSnapshotRestore(t *testing.T) {
	f, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	raftEntry(t, f, 1, command{Operation: opSet, Namespace: "settings", Key: "abc", Value: []byte("snapshot-value")})
	raftEntry(t, f, 2, command{Operation: opPeer, Peer: &peerInfo{ID: "node0", RaftAddr: "node0:7000", GRPCAddr: "node0:50051", AdminAddr: "node0:7001"}})
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	snapshot.Release()
	if !sink.closed || sink.canceled {
		t.Fatal("snapshot sink wasn't finalized")
	}
	raftEntry(t, f, 3, command{Operation: opDelete, Namespace: "settings", Key: "abc"})
	if err := f.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	got, found, err := f.Get(context.Background(), "settings", "abc")
	if err != nil || !found || string(got.Value) != "snapshot-value" {
		t.Fatalf("restore failed: %+v found=%v err=%v", got, found, err)
	}
	if f.applied != 2 || f.revision != 1 {
		t.Fatalf("restore counters applied=%d revision=%d", f.applied, f.revision)
	}
	peer, ok, err := f.Peer("node0")
	if err != nil || !ok || peer.GRPCAddr != "node0:50051" {
		t.Fatalf("restore peer: %+v, found=%v err=%v", peer, ok, err)
	}
}

func TestFSMUnknownCommandFailsClosed(t *testing.T) {
	f, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out, ok := f.Apply(&raft.Log{Index: 1, Data: []byte(`{"op":"unknown"}`)}).(applyResult)
	if !ok || out.Err == nil || f.Failure() == nil {
		t.Fatal("expected fatal FSM apply failure")
	}
}
