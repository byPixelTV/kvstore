package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/storage"
	pebbledb "github.com/cockroachdb/pebble/v2"
)

// A reserved metadata key. User records always start with 0x01.
var revisionKey = []byte{0x00, 0x01}

// Store is a persistent, single-node implementation of storage.Store.
// One dedicated worker serializes changes while grouping concurrent writes
// into atomic WAL-synced Pebble batches.
type Store struct {
	mu           sync.RWMutex
	readMu       sync.RWMutex
	published    *pebbledb.Snapshot
	gate         sync.RWMutex
	closed       bool
	failed       error
	writeQueue   chan writeRequest
	workerDone   chan struct{}
	batchWindow  time.Duration
	batchMax     int
	db           *pebbledb.DB
	revision     uint64
	oldest       uint64
	historyBytes uint64
	notifier     changefeed.Notifier
}

var _ storage.Store = (*Store)(nil)

// Options control bounded micro-batching. A zero Options value uses defaults.
type Options struct {
	BatchWindow time.Duration
	BatchMax    int
}

func Open(path string) (*Store, error) { return OpenWithOptions(path, Options{}) }

func OpenWithOptions(path string, opts Options) (*Store, error) {
	if opts.BatchWindow == 0 {
		opts.BatchWindow = defaultWriteBatchWindow
	}
	if opts.BatchMax == 0 {
		opts.BatchMax = defaultWriteBatchMax
	}
	if opts.BatchWindow < 0 || opts.BatchWindow > 20*time.Millisecond || opts.BatchMax < 1 || opts.BatchMax > 4096 {
		return nil, errors.New("invalid write batch settings")
	}
	db, err := pebbledb.Open(path, nil)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}
	store := &Store{db: db, writeQueue: make(chan writeRequest, writeQueueCapacity), workerDone: make(chan struct{}), batchWindow: opts.BatchWindow, batchMax: opts.BatchMax}
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
	// Published reads never observe a write before its WAL sync completes.
	store.published = db.NewSnapshot()
	go store.writeLoop()
	return store, nil
}

func (s *Store) Close() error {
	// Reject new work, drain accepted work, then close the DB.
	s.gate.Lock()
	if s.closed {
		s.gate.Unlock()
		<-s.workerDone
		return nil
	}
	s.closed = true
	close(s.writeQueue)
	s.gate.Unlock()
	<-s.workerDone
	s.readMu.Lock()
	if s.published != nil {
		_ = s.published.Close()
		s.published = nil
	}
	defer s.readMu.Unlock()
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

func (s *Store) Get(ctx context.Context, namespace, key string) (storage.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return storage.Record{}, false, err
	}
	// Reads use a published snapshot advanced only after a successful WAL
	// sync. They never wait behind an in-flight fsync.
	s.readMu.RLock()
	defer s.readMu.RUnlock()
	raw, closer, err := s.published.Get(encodeKey(namespace, key))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return storage.Record{}, false, nil
	}
	if err != nil {
		return storage.Record{}, false, fmt.Errorf("pebble get: %w", err)
	}
	// Pebble's value is only valid until its closer is closed.
	record, err := storage.DecodeRecord(raw)
	found := err == nil && (record.ExpiresAtUnixMS == 0 || !record.ExpiredAt(time.Now().UnixMilli()))
	if found {
		record.Value = bytes.Clone(record.Value)
	}
	if err = errors.Join(err, closer.Close()); err != nil || !found {
		return storage.Record{}, false, err
	}
	return record, true, nil
}

func (s *Store) Set(ctx context.Context, namespace, key string, value []byte) (uint64, error) {
	return s.SetWithExpiration(ctx, namespace, key, value, 0)
}

func (s *Store) SetWithExpiration(ctx context.Context, namespace, key string, value []byte, expiresAtUnixMS int64) (uint64, error) {
	if expiresAtUnixMS < 0 {
		return 0, errors.New("invalid expiration")
	}
	// Clone before passing to a worker: the caller can modify the slice.
	return s.submit(ctx, setKind, namespace, key, bytes.Clone(value), expiresAtUnixMS)
}

func (s *Store) Delete(ctx context.Context, namespace, key string) (uint64, error) {
	return s.submit(ctx, deleteKind, namespace, key, nil, 0)
}

// ReadChanges serves durable events from the exact same Pebble commits as Set/Delete.
func (s *Store) ReadChanges(ctx context.Context, after uint64, limit int) (changefeed.Page, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.failed != nil {
		return changefeed.Page{}, s.failed
	}
	return changefeed.Read(ctx, s.db, after, s.revision, s.oldest, limit)
}
func (s *Store) Notify() <-chan struct{} { return s.notifier.Subscribe() }

func (s *Store) CompactionCounts() (completed, failed int64) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if s.closed {
		return 0, 0
	}
	metrics := s.db.Metrics()
	return metrics.Compact.Count, metrics.Compact.FailedCount
}

func (s *Store) Head(ctx context.Context) (uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.failed != nil {
		return 0, 0, s.failed
	}
	return s.revision, s.oldest, nil
}

func (s *Store) ScanNamespace(ctx context.Context, namespace string, emit func(storage.ScanItem) error) error {
	s.mu.RLock()
	if s.failed != nil {
		err := s.failed
		s.mu.RUnlock()
		return err
	}
	snapshot := s.db.NewSnapshot()
	head := s.revision
	s.mu.RUnlock()
	defer snapshot.Close()
	return changefeed.ScanSnapshot(ctx, snapshot, head, namespace, emit)
}

func (s *Store) ListNamespaces(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	if s.failed != nil {
		err := s.failed
		s.mu.RUnlock()
		return nil, err
	}
	snapshot := s.db.NewSnapshot()
	s.mu.RUnlock()
	defer snapshot.Close()
	return changefeed.ListNamespaces(ctx, snapshot)
}
