package changefeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func TestBatchWriterEvictsNewlyStagedEvents(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old := Encode(Change{Revision: 1, Namespace: "n", Key: "k", Value: []byte("v")})
	if err := db.Set(Key(1), old, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	event2 := Change{Revision: 2, Namespace: "n", Key: "k", Value: []byte("v")}
	event3 := Change{Revision: 3, Namespace: "n", Key: "k", Value: []byte("v")}
	// Synthetic near-limit history budget: first evicts persisted #1, then
	// #3 evicts the uncommitted #2 staged in this same Pebble batch.
	used := MaxHistoryBytes - uint64(len(Encode(event2))) + uint64(len(old)) - 1
	batch := db.NewBatch()
	writer := NewBatchWriter(db, batch, 1, used, true)
	if err := writer.Append(event2); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(event3); err != nil {
		t.Fatal(err)
	}
	floor, _ := writer.State()
	if floor != 3 {
		t.Fatalf("retention floor=%d want 3", floor)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = batch.Close()
	page, err := Read(context.Background(), db, 2, 3, floor, 10)
	if err != nil || len(page.Events) != 1 || page.Events[0].Revision != 3 {
		t.Fatalf("events=%+v err=%v", page, err)
	}
}
