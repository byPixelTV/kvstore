package changefeed

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// BatchWriter stages multiple adjacent revisions in ONE Pebble batch. It
// tracks uncommitted event sizes so retention can evict an event written
// earlier in the same batch. Never reuse after committing/closing the batch.
// The owner must serialize writers and publish Head/Oldest only after commit.
type BatchWriter struct {
	db           *pebble.DB
	batch        *pebble.Batch
	oldest, used uint64
	hasFloor     bool
	pending      map[uint64]uint64
	floorDirty   bool
	bytesDirty   bool
}

func NewBatchWriter(db *pebble.DB, batch *pebble.Batch, oldest, used uint64, hasFloor bool) *BatchWriter {
	return &BatchWriter{db: db, batch: batch, oldest: oldest, used: used, hasFloor: hasFloor, pending: make(map[uint64]uint64)}
}

func (w *BatchWriter) State() (oldest, used uint64) { return w.oldest, w.used }

func (w *BatchWriter) Append(event Change) error {
	if w.oldest == 0 || event.Revision < w.oldest {
		return errors.New("invalid changefeed append cursor")
	}
	raw := Encode(event)
	if uint64(len(raw)) > MaxHistoryBytes {
		return errors.New("event exceeds history byte limit")
	}
	if w.used > ^uint64(0)-uint64(len(raw)) {
		return errors.New("history bytes overflow")
	}
	if err := w.batch.Set(Key(event.Revision), raw, nil); err != nil {
		return err
	}
	w.pending[event.Revision] = uint64(len(raw))
	w.used += uint64(len(raw))

	original := w.oldest
	for event.Revision-w.oldest+1 > MaxEvents || w.used > MaxHistoryBytes {
		var size uint64
		if staged, ok := w.pending[w.oldest]; ok {
			size = staged
			delete(w.pending, w.oldest)
		} else {
			stale, closer, err := w.db.Get(Key(w.oldest))
			if err != nil {
				return fmt.Errorf("read retained event %d: %w", w.oldest, err)
			}
			size = uint64(len(stale))
			if err := closer.Close(); err != nil {
				return err
			}
		}
		if size > w.used {
			return errors.New("corrupt retained event byte count")
		}
		if err := w.batch.Delete(Key(w.oldest), nil); err != nil {
			return err
		}
		w.used -= size
		w.oldest++
	}
	// Metadata is only written once per COMMIT, not once per event. Each
	// 128-write Raft batch previously staged 128 redundant byte-count sets.
	// Call Flush before committing the Pebble batch.
	if !w.hasFloor || original != w.oldest {
		w.floorDirty = true
		w.hasFloor = true
	}
	w.bytesDirty = true
	return nil
}

// Flush must be called once after all Append calls and BEFORE the owning
// Pebble batch is committed. It leaves all revision/event mutations in the
// caller's same atomic, durable commit; no separate metadata fsync occurs.
func (w *BatchWriter) Flush() error {
	if w.floorDirty {
		if err := w.batch.Set(FloorKey, EncodedFloor(w.oldest), nil); err != nil {
			return err
		}
		w.floorDirty = false
	}
	if w.bytesDirty {
		if err := w.batch.Set(ByteCountKey, EncodedFloor(w.used), nil); err != nil {
			return err
		}
		w.bytesDirty = false
	}
	return nil
}
