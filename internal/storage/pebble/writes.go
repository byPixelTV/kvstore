package pebblestore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/byPixelTV/kvstore/internal/changefeed"
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
}

type writeResponse struct {
	revision uint64
	err      error
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
		encodedKey := encodeKey(req.namespace, req.key)
		key := string(encodedKey)
		if req.kind == deleteKind {
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
		if req.kind == setKind {
			batchErr = batch.Set(encodedKey, encodeRecord(req.value, next), nil)
			shadow[key] = true
		} else {
			batchErr = batch.Delete(encodedKey, nil)
			shadow[key] = false
		}
		if batchErr != nil {
			break
		}
		batchErr = writer.Append(changefeed.Change{Revision: next, Namespace: req.namespace, Key: req.key, Value: req.value, Deleted: req.kind == deleteKind})
		if batchErr != nil {
			break
		}
		results[i].revision = next
		changed = true
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
