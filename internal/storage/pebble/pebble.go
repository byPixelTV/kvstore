package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/byPixelTV/kvstore/internal/storage"
	pebbledb "github.com/cockroachdb/pebble/v2"
)

// A reserved metadata key. User records always start with 0x01.
var revisionKey = []byte{0x00, 0x01}

// Store is a persistent, single-node implementation of storage.Store.
// The mutex serializes writes so every mutation receives a unique revision.
type Store struct {
	mu       sync.Mutex
	db       *pebbledb.DB
	revision uint64
}

var _ storage.Store = (*Store)(nil)

func Open(path string) (*Store, error) {
	db, err := pebbledb.Open(path, nil)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	store := &Store{db: db}
	raw, closer, err := db.Get(revisionKey)
	switch {
	case errors.Is(err, pebbledb.ErrNotFound):
		return store, nil
	case err != nil:
		_ = db.Close()
		return nil, fmt.Errorf("read revision: %w", err)
	}

	if len(raw) != 8 {
		_ = closer.Close()
		_ = db.Close()
		return nil, fmt.Errorf("invalid stored revision length: %d", len(raw))
	}
	store.revision = binary.BigEndian.Uint64(raw)
	if err := closer.Close(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("close revision reader: %w", err)
	}
	return store, nil
}

func (s *Store) Close() error {
	// Call only after the gRPC server has finished handling requests.
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// encodeKey uniquely encodes the namespace and key without delimiter collisions.
func encodeKey(namespace, key string) []byte {
	encoded := binary.AppendUvarint([]byte{0x01}, uint64(len(namespace)))
	encoded = append(encoded, namespace...)
	return append(encoded, key...)
}

func encodeRecord(value []byte, revision uint64) []byte {
	result := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(result[:8], revision)
	copy(result[8:], value)
	return result
}

func (s *Store) Get(ctx context.Context, namespace, key string) (storage.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return storage.Record{}, false, err
	}

	raw, closer, err := s.db.Get(encodeKey(namespace, key))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return storage.Record{}, false, nil
	}
	if err != nil {
		return storage.Record{}, false, fmt.Errorf("pebble get: %w", err)
	}
	// Pebble's value is only valid until its closer is closed.
	if len(raw) < 8 {
		_ = closer.Close()
		return storage.Record{}, false, errors.New("corrupt record: missing revision")
	}
	record := storage.Record{
		Revision: binary.BigEndian.Uint64(raw[:8]),
		Value:    bytes.Clone(raw[8:]),
	}
	if err := closer.Close(); err != nil {
		return storage.Record{}, false, fmt.Errorf("close pebble reader: %w", err)
	}
	return record, true, nil
}

func (s *Store) Set(ctx context.Context, namespace, key string, value []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.revision == math.MaxUint64 {
		return 0, errors.New("revision counter exhausted")
	}

	next := s.revision + 1
	batch := s.db.NewBatch()
	defer func(batch *pebbledb.Batch) {
		err := batch.Close()
		if err != nil {
			slog.Error("failed to close batch", "error", err)
		}
	}(batch)

	if err := batch.Set(encodeKey(namespace, key), encodeRecord(value, next), nil); err != nil {
		return 0, fmt.Errorf("batch set record: %w", err)
	}
	var encodedRevision [8]byte
	binary.BigEndian.PutUint64(encodedRevision[:], next)
	if err := batch.Set(revisionKey, encodedRevision[:], nil); err != nil {
		return 0, fmt.Errorf("batch set revision: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := batch.Commit(pebbledb.Sync); err != nil {
		return 0, fmt.Errorf("commit set: %w", err)
	}
	s.revision = next
	return next, nil
}

func (s *Store) Delete(ctx context.Context, namespace, key string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}
	encodedKey := encodeKey(namespace, key)
	_, closer, err := s.db.Get(encodedKey)
	if errors.Is(err, pebbledb.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("pebble get before delete: %w", err)
	}
	if err := closer.Close(); err != nil {
		return 0, fmt.Errorf("close pebble reader: %w", err)
	}
	if s.revision == math.MaxUint64 {
		return 0, errors.New("revision counter exhausted")
	}

	next := s.revision + 1
	batch := s.db.NewBatch()
	defer func(batch *pebbledb.Batch) {
		err := batch.Close()
		if err != nil {
			slog.Error("failed to close batch", "error", err)
		}
	}(batch)
	if err := batch.Delete(encodedKey, nil); err != nil {
		return 0, fmt.Errorf("batch delete record: %w", err)
	}
	var encodedRevision [8]byte
	binary.BigEndian.PutUint64(encodedRevision[:], next)
	if err := batch.Set(revisionKey, encodedRevision[:], nil); err != nil {
		return 0, fmt.Errorf("batch set revision: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := batch.Commit(pebbledb.Sync); err != nil {
		return 0, fmt.Errorf("commit delete: %w", err)
	}
	s.revision = next
	return next, nil
}
