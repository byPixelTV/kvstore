package cluster

import (
	"context"
	"testing"

	"github.com/byPixelTV/kvstore/internal/storage"
)

func TestRaftFSMChangefeedSurvivesRestart(t *testing.T) {
	path := t.TempDir()
	ctx := context.Background()
	f, err := openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	raftEntry(t, f, 1, command{Operation: opSet, Namespace: "n", Key: "a", Value: []byte("v")})
	raftEntry(t, f, 2, command{Operation: opSet, Namespace: "n", Key: "b", Value: []byte("w")})
	raftEntry(t, f, 3, command{Operation: opDelete, Namespace: "n", Key: "a"})
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = openFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	page, err := f.ReadChanges(ctx, 0, 100)
	if err != nil || page.Head != 3 || len(page.Events) != 3 || !page.Events[2].Deleted {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	head, oldest, err := f.Head(ctx)
	if err != nil || head != 3 || oldest != 1 {
		t.Fatalf("head=%d oldest=%d err=%v", head, oldest, err)
	}
	n := 0
	err = f.ScanNamespace(ctx, "n", func(item storage.ScanItem) error {
		if item.Checkpoint {
			if item.SnapshotRevision != 3 {
				t.Fatalf("checkpoint=%d", item.SnapshotRevision)
			}
		} else {
			n++
			if item.Key != "b" {
				t.Fatalf("unexpected scan entry %+v", item)
			}
		}
		return nil
	})
	if err != nil || n != 1 {
		t.Fatalf("scan items=%d err=%v", n, err)
	}
}
