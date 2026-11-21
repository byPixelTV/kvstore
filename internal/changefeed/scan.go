package changefeed

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/cockroachdb/pebble/v2"
)

// ScanSnapshot holds a DB snapshot that was acquired together with its
// global revision under the writer lock. The callback never holds that lock.
func ScanSnapshot(ctx context.Context, snap *pebble.Snapshot, head uint64, namespace string, emit func(storage.ScanItem) error) error {
	asOf := time.Now().UnixMilli()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := emit(storage.ScanItem{SnapshotRevision: head, Checkpoint: true}); err != nil {
		return err
	}
	prefix := binary.AppendUvarint([]byte{1}, uint64(len(namespace)))
	prefix = append(prefix, namespace...)
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 255 {
			upper[i]++
			upper = upper[:i+1]
			break
		}
	}
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return err
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Key()
		if len(key) < len(prefix) {
			return errors.New("corrupt namespace key")
		}
		record, err := storage.DecodeRecord(it.Value())
		if err != nil {
			return fmt.Errorf("corrupt snapshot record at %x: %w", key, err)
		}
		if record.ExpiredAt(asOf) {
			continue
		}
		if err := emit(storage.ScanItem{SnapshotRevision: head, Key: string(key[len(prefix):]), Value: append([]byte(nil), record.Value...), Revision: record.Revision, ExpiresAtUnixMS: record.ExpiresAtUnixMS}); err != nil {
			return err
		}
	}
	return it.Error()
}
