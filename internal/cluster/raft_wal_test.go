package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func TestRaftWALOnlyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := openRaftWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !w.IsMonotonic() {
		t.Fatal("raft-wal must support monotonic log indexes")
	}
	metrics := &measuredLogStore{inner: w, stats: &raftLogMetrics{}}
	if !metrics.IsMonotonic() {
		t.Fatal("measured Raft log store must preserve monotonicity")
	}
	if err := w.SetUint64([]byte("term"), 42); err != nil {
		t.Fatal(err)
	}
	if err := metrics.StoreLog(&raft.Log{Index: 1, Term: 42, Type: raft.LogCommand, Data: []byte("persistent")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = openRaftWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	term, err := w.GetUint64([]byte("term"))
	if err != nil || term != 42 {
		t.Fatalf("stable metadata: term=%d err=%v", term, err)
	}
	index, err := w.LastIndex()
	if err != nil || index != 1 {
		t.Fatalf("last index=%d err=%v", index, err)
	}
	var l raft.Log
	if err := w.GetLog(index, &l); err != nil || string(l.Data) != "persistent" {
		t.Fatalf("recovered log: %+v err=%v", l, err)
	}
}

func TestRaftWALRefusesLegacyBoltWithoutDeletingIt(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "raft.db")
	if err := os.WriteFile(old, []byte("legacy data"), 0600); err != nil {
		t.Fatal(err)
	}
	w, err := openRaftWAL(dir)
	if w != nil {
		_ = w.Close()
		t.Fatal("unexpected fallback or migration")
	}
	if err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("expected explicit legacy-format error, got %v", err)
	}
	data, err := os.ReadFile(old)
	if err != nil || string(data) != "legacy data" {
		t.Fatal("legacy storage must not be modified")
	}
}
