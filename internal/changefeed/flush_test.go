package changefeed

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func TestBatchWriterFlushPersistsCorrectMetadataAndEvents(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := db.NewBatch()
	w := NewBatchWriter(db, b, 1, 0, false)
	var expected uint64
	for i := uint64(1); i <= 64; i++ {
		e := Change{Revision: i, Namespace: "ns", Key: "k", Value: []byte("value")}
		expected += uint64(len(Encode(e)))
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	raw, closer, err := db.Get(ByteCountKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 8 || binary.BigEndian.Uint64(raw) != expected {
		t.Fatalf("corrupt byte counter: %x expected %d", raw, expected)
	}
	_ = closer.Close()
	page, err := Read(context.Background(), db, 0, 64+1, 1, 100)
	if err != nil || len(page.Events) != 64 {
		t.Fatalf("durable history size=%d err=%v", len(page.Events), err)
	}
}
