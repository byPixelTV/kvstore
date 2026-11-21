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
	"github.com/byPixelTV/kvstore/internal/transaction"
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
	ctx             context.Context
	kind            writeKind
	namespace, key  string
	value           []byte
	expiresAtUnixMS int64
	reply           chan writeResponse
	operations      []storage.WriteOperation
	transaction     *transaction.Plan
}

type writeResponse struct {
	revision  uint64
	revisions []uint64
	outcome   transaction.Outcome
	err       error
}

func (s *Store) submit(ctx context.Context, kind writeKind, namespace, key string, value []byte, expiresAtUnixMS int64) (uint64, error) {
	result := s.submitRequest(writeRequest{ctx: ctx, kind: kind, namespace: namespace, key: key, value: value, expiresAtUnixMS: expiresAtUnixMS})
	return result.revision, result.err
}

// submitRequest holds the gate while enqueuing so Close drains accepted work.
// An enqueued request may still commit after the caller cancels.
func (s *Store) submitRequest(req writeRequest) writeResponse {
	ctx := req.ctx
	if err := ctx.Err(); err != nil {
		return writeResponse{err: err}
	}
	req.reply = make(chan writeResponse, 1)
	s.gate.RLock()
	if s.closed {
		s.gate.RUnlock()
		return writeResponse{err: ErrStoreClosed}
	}
	select {
	case s.writeQueue <- req:
		s.gate.RUnlock()
	case <-ctx.Done():
		s.gate.RUnlock()
		return writeResponse{err: ctx.Err()}
	}
	select {
	case response := <-req.reply:
		return response
	case <-ctx.Done():
		return writeResponse{err: ctx.Err()}
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
		if op.ExpiresAtUnixMS < 0 || (op.Delete && op.ExpiresAtUnixMS != 0) {
			return nil, errors.New("invalid batch expiration")
		}
		total += len(op.Namespace) + len(op.Key) + len(op.Value) + 128
		if total > 256<<10 {
			return nil, errors.New("batch exceeds 256 KiB")
		}
		op.Value = bytes.Clone(op.Value)
		copyOps[i] = op
	}
	result := s.submitRequest(writeRequest{ctx: ctx, operations: copyOps})
	return result.revisions, result.err
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
// sequentially against the effects of earlier writes in the group.
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
	batch := s.newWriteBatch(group)
	shadow := make(map[string]bool, len(group))
	writer := changefeed.NewBatchWriter(s.db, batch, s.oldest, s.historyBytes, true)
	next := s.revision
	changed := false
	receiptsWritten := false
	var batchErr error

	for i, req := range group {
		if err := req.ctx.Err(); err != nil {
			results[i].err = err
			continue
		}
		ops := req.operations
		if req.transaction != nil {
			outcome, staged, err := stageLocalTransaction(batch, req.transaction, time.Now().UnixMilli())
			if errors.Is(err, errTransactionIDConflict) {
				results[i].err = err
				continue
			}
			if err != nil {
				batchErr = err
				break
			}
			results[i].outcome = outcome
			receiptsWritten = receiptsWritten || staged
			if !staged || outcome != transaction.Committed {
				continue
			}
			ops = req.transaction.Parts[0].Writes
		} else if ops == nil {
			ops = []storage.WriteOperation{{Namespace: req.namespace, Key: req.key, Value: req.value, Delete: req.kind == deleteKind, ExpiresAtUnixMS: req.expiresAtUnixMS}}
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
				batchErr = batch.Set(encodedKey, storage.EncodeRecord(op.Value, next, op.ExpiresAtUnixMS), nil)
				shadow[key] = true
			} else {
				batchErr = batch.Delete(encodedKey, nil)
				shadow[key] = false
			}
			if batchErr != nil {
				break
			}
			batchErr = writer.Append(changefeed.Change{Revision: next, Namespace: op.Namespace, Key: op.Key, Value: op.Value, Deleted: op.Delete, ExpiresAtUnixMS: op.ExpiresAtUnixMS})
			if batchErr != nil {
				break
			}
			revisions[j] = next
			changed = true
		}
		if batchErr != nil {
			break
		}
		if req.transaction != nil {
			continue
		}
		if req.operations != nil {
			results[i].revisions = revisions
		} else {
			results[i].revision = revisions[0]
		}
	}

	commitAttempted := false
	if batchErr == nil && changed {
		batchErr = writer.Flush()
	}
	if batchErr == nil && changed {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], next)
		batchErr = batch.Set(revisionKey, encoded[:], nil)
	}
	if batchErr == nil && (changed || receiptsWritten) {
		commitAttempted = true
		batchErr = batch.Commit(pebbledb.Sync)
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
		s.readMu.Unlock()
		// All readers using the old snapshot finished before the swap.
		// Do not stall new GETs while Pebble releases snapshot resources.
		_ = old.Close()
		s.revision = next
		s.oldest, s.historyBytes = writer.State()
		s.notifier.Signal()
	}
	s.mu.Unlock()
	for i, req := range group {
		req.reply <- results[i]
	}
}
