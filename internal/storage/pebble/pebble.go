package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/storage"
	pebbledb "github.com/cockroachdb/pebble/v2"
)

// A reserved metadata key. User records always start with 0x01.
var revisionKey = []byte{0x00, 0x01}

// Store is a persistent, single-node implementation of storage.Store.
// The mutex serializes writes so every mutation receives a unique revision.
type Store struct {
	mu           sync.Mutex
	db           *pebbledb.DB
	revision     uint64
	oldest       uint64
	historyBytes uint64
	notifier     changefeed.Notifier
}

var _ storage.Store = (*Store)(nil)

func Open(path string) (*Store, error) {
	db, err := pebbledb.Open(path, nil)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}
	store := &Store{db: db}
	read := func(key []byte) (uint64, bool, error) {
		raw, closer, err := db.Get(key)
		if errors.Is(err, pebbledb.ErrNotFound) {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, err
		}
		defer closer.Close()
		if len(raw) != 8 {
			return 0, false, fmt.Errorf("invalid metadata at %x", key)
		}
		return binary.BigEndian.Uint64(raw), true, nil
	}
	rev, _, err := read(revisionKey)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store.revision = rev
	floor, exists, err := read(changefeed.FloorKey)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if !exists {
		if rev == math.MaxUint64 {
			_ = db.Close()
			return nil, errors.New("revision exhausted")
		}
		floor = rev + 1 // existing data has no prior changefeed history
		if err = db.Set(changefeed.FloorKey, changefeed.EncodedFloor(floor), pebbledb.Sync); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if floor == 0 || floor > rev+1 {
		_ = db.Close()
		return nil, errors.New("invalid changefeed floor")
	}
	store.oldest = floor
	history, _, err := read(changefeed.ByteCountKey)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store.historyBytes = history
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
	defer batch.Close()

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
	floor, history, err := changefeed.Append(s.db, batch, changefeed.Change{Revision: next, Namespace: namespace, Key: key, Value: value}, s.oldest, s.historyBytes, true)
	if err != nil {
		return 0, err
	}
	if err := batch.Commit(pebbledb.Sync); err != nil {
		return 0, fmt.Errorf("commit set: %w", err)
	}
	s.revision = next
	s.oldest = floor
	s.historyBytes = history
	s.notifier.Signal()
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
	defer batch.Close()
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
	floor, history, err := changefeed.Append(s.db, batch, changefeed.Change{Revision: next, Namespace: namespace, Key: key, Deleted: true}, s.oldest, s.historyBytes, true)
	if err != nil {
		return 0, err
	}
	if err := batch.Commit(pebbledb.Sync); err != nil {
		return 0, fmt.Errorf("commit delete: %w", err)
	}
	s.revision = next
	s.oldest = floor
	s.historyBytes = history
	s.notifier.Signal()
	return next, nil
}

// ReadChanges serves durable events from the exact same Pebble commits as Set/Delete.
func (s *Store) ReadChanges(ctx context.Context, after uint64, limit int) (changefeed.Page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return changefeed.Read(ctx, s.db, after, s.revision, s.oldest, limit)
}
func (s *Store) Notify() <-chan struct{} { return s.notifier.Subscribe() }

func (s *Store) Head(ctx context.Context) (uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision, s.oldest, nil
}

func (s *Store) ScanNamespace(ctx context.Context, namespace string, emit func(storage.ScanItem) error) error {
	s.mu.Lock()
	snapshot := s.db.NewSnapshot()
	head := s.revision
	s.mu.Unlock()
	defer snapshot.Close()
	return changefeed.ScanSnapshot(ctx, snapshot, head, namespace, emit)
}
