package changefeed

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
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
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: afterPrefix(prefix)})
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

// ListNamespaces visits the first live record for each namespace in the
// snapshot. A seek skips the remaining records once the name is known.
func ListNamespaces(ctx context.Context, snap *pebble.Snapshot) ([]string, error) {
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: []byte{1}, UpperBound: []byte{2}})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	asOf := time.Now().UnixMilli()
	var names []string
	for ok := it.First(); ok; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := it.Key()
		length, width := binary.Uvarint(key[1:])
		if width < 1 || length == 0 || length >= uint64(len(key)-1-width) {
			return nil, errors.New("corrupt namespace key")
		}
		end := 1 + width + int(length)
		record, err := storage.DecodeRecord(it.Value())
		if err != nil {
			return nil, fmt.Errorf("corrupt namespace record: %w", err)
		}
		if record.ExpiredAt(asOf) {
			ok = it.Next()
			continue
		}
		names = append(names, string(key[1+width:end]))
		ok = it.SeekGE(afterPrefix(key[:end]))
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	slices.Sort(names)
	return names, ctx.Err()
}

func afterPrefix(prefix []byte) []byte {
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 255 {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}
