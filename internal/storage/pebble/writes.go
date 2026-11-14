package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/storage"
	pebbledb "github.com/cockroachdb/pebble/v2"
)

const (
	defaultWriteBatchMax    = 64
	defaultWriteBatchWindow = 250 * time.Microsecond
	writeQueueCapacity      = 4096
)

var ErrStoreClosed = errors.New("pebble store is closed")

type writeKind uint8

const (
	setKind writeKind = iota
	deleteKind
)

type writeRequest struct {
	ctx            context.Context
	kind           writeKind
	namespace, key string
	value          []byte
	reply          chan writeResponse
	operations     []storage.WriteOperation
}

type writeResponse struct {
	revision  uint64
	revisions []uint64
	err       error
}

// submit never loses an accepted write during Close: the gate's read lock
// prevents Close from closing the queue while a sender is still enqueuing.
// A canceled request already enqueued may still commit; like raft.Apply,
// canceled or timed-out writes must be treated as outcome-unknown.
func (s *Store) submit(ctx context.Context, kind writeKind, namespace, key string, value []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	req := writeRequest{ctx: ctx, kind: kind, namespace: namespace, key: key, value: value, reply: make(chan writeResponse, 1)}
	s.gate.RLock()
	if s.closed {
		s.gate.RUnlock()
		return 0, ErrStoreClosed
	}
	select {
	case s.writeQueue <- req:
		s.gate.RUnlock()
	case <-ctx.Done():
		s.gate.RUnlock()
		return 0, ctx.Err()
	}
	select {
	case response := <-req.reply:
		return response.revision, response.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// BatchWrite enqueues the complete request as ONE unit, ensuring that its
// operations share a single durable Pebble sync with preserved item order.
func (s *Store) BatchWrite(ctx context.Context, operations []storage.WriteOperation) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(operations) < 1 || len(operations) > 128 {
		return nil, errors.New("invalid batch length")
	}
	total := 0
	copyOps := make([]storage.WriteOperation, len(operations))
	for i, op := range operations {
		if op.Namespace == "" || op.Key == "" {
			return nil, errors.New("empty namespace/key")
		}
		total += len(op.Namespace) + len(op.Key) + len(op.Value) + 128
		if total > 256<<10 {
			return nil, errors.New("batch exceeds 256 KiB")
		}
		copyOps[i] = storage.WriteOperation{Namespace: op.Namespace, Key: op.Key, Delete: op.Delete, Value: bytes.Clone(op.Value)}
	}
	req := writeRequest{ctx: ctx, operations: copyOps, reply: make(chan writeResponse, 1)}
	s.gate.RLock()
	if s.closed {
		s.gate.RUnlock()
		return nil, ErrStoreClosed
	}
	select {
	case s.writeQueue <- req:
		s.gate.RUnlock()
	case <-ctx.Done():
		s.gate.RUnlock()
		return nil, ctx.Err()
	}
	select {
	case result := <-req.reply:
		return result.revisions, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Store) writeLoop() {
	defer close(s.workerDone)
	for {
		first, ok := <-s.writeQueue
		if !ok {
			return
		}
		group := []writeRequest{first}
		timer := time.NewTimer(s.batchWindow)
		stopped := false
	collect:
		for len(group) < s.batchMax {
			select {
			case req, ok := <-s.writeQueue:
				if !ok {
					stopped = true
					break collect
				}
				group = append(group, req)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		s.applyGroup(group)
		if stopped {
			return
		}
	}
}

// applyGroup is the only mutation writer. Every accepted operation is evaluated
// sequentially against an in-batch shadow map, not against stale DB state.
// One WAL sync durably publishes all successful operations and their feed
// records; replies are sent only AFTER that sync completes.
func (s *Store) applyGroup(group []writeRequest) {
	results := make([]writeResponse, len(group))
	s.mu.Lock()
	if s.failed != nil {
		for i := range results {
			results[i].err = s.failed
		}
		s.mu.Unlock()
		for i, req := range group {
			req.reply <- results[i]
		}
		return
	}
	batch := s.db.NewBatch()
	shadow := make(map[string]bool, len(group))
	writer := changefeed.NewBatchWriter(s.db, batch, s.oldest, s.historyBytes, true)
	next := s.revision
	changed := false
	var batchErr error

	for i, req := range group {
		if err := req.ctx.Err(); err != nil {
			results[i].err = err
			continue
		}
		ops := req.operations
		if ops == nil {
			ops = []storage.WriteOperation{{Namespace: req.namespace, Key: req.key, Value: req.value, Delete: req.kind == deleteKind}}
		}
		revisions := make([]uint64, len(ops))
		for j, op := range ops {
			encodedKey := encodeKey(op.Namespace, op.Key)
			key := string(encodedKey)
			if op.Delete {
				exists, seen := shadow[key]
				if !seen {
					_, closer, err := s.db.Get(encodedKey)
					if errors.Is(err, pebbledb.ErrNotFound) {
						exists = false
					} else if err != nil {
						batchErr = fmt.Errorf("read before delete: %w", err)
						break
					} else {
						exists = true
						if err := closer.Close(); err != nil {
							batchErr = err
							break
						}
					}
				}
				if !exists {
					continue
				}
			}
			if next == math.MaxUint64 {
				batchErr = errors.New("revision counter exhausted")
				break
			}
			next++
			if !op.Delete {
				batchErr = batch.Set(encodedKey, encodeRecord(op.Value, next), nil)
				shadow[key] = true
			} else {
				batchErr = batch.Delete(encodedKey, nil)
				shadow[key] = false
			}
			if batchErr != nil {
				break
			}
			batchErr = writer.Append(changefeed.Change{Revision: next, Namespace: op.Namespace, Key: op.Key, Value: op.Value, Deleted: op.Delete})
			if batchErr != nil {
				break
			}
			revisions[j] = next
			changed = true
		}
		if batchErr != nil {
			break
		}
		if req.operations != nil {
			results[i].revisions = revisions
		} else {
			results[i].revision = revisions[0]
		}
	}

	commitAttempted := false
	if batchErr == nil && changed {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], next)
		batchErr = batch.Set(revisionKey, encoded[:], nil)
		if batchErr == nil {
			commitAttempted = true
			batchErr = batch.Commit(pebbledb.Sync)
		}
	}
	_ = batch.Close()
	if batchErr != nil {
		// A rejected input or a staging error does not poison the database:
		// the uncommitted batch was discarded. A failed *commit* is fatal,
		// because the durable outcome may be uncertain.
		if commitAttempted {
			s.failed = fmt.Errorf("commit write group: %w", batchErr)
			batchErr = s.failed
		}
		for i := range results {
			results[i] = writeResponse{err: batchErr}
		}
	} else if changed {
		// Publish a new read snapshot only after the WAL is durable.
		snapshot := s.db.NewSnapshot()
		s.readMu.Lock()
		old := s.published
		s.published = snapshot
		_ = old.Close() // no readers hold the previous snapshot
		s.readMu.Unlock()
		s.revision = next
		s.oldest, s.historyBytes = writer.State()
		s.notifier.Signal()
	}
	s.mu.Unlock()
	for i, req := range group {
		req.reply <- results[i]
	}
}
