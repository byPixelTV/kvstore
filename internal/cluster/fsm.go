package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/cockroachdb/pebble/v2"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These key prefixes match the existing standalone Pebble record encoding.
// Cluster data lives in /data/state, separately from the standalone /data DB.
var (
	revisionKey = []byte{0, 1}
	appliedKey  = []byte{0, 2}
)

const snapshotMagic = "KVSTORE-SNAPSHOT-1\n"
const maxSnapshotField = 64 << 20 // 64 MiB per snapshot key or value

const (
	opSet    = "set"
	opDelete = "delete"
	opPeer   = "peer"
	opBatch  = "batch"
	// Each operation in a microbatch belongs to an independent point RPC.
	// A migration fence must reject only the affected request, not its peers.
	opMicrobatch = "microbatch"
	opFence      = "slot-fence"
)

type peerInfo struct {
	ID        string `json:"id"`
	RaftAddr  string `json:"raft_addr"`
	GRPCAddr  string `json:"grpc_addr"`
	AdminAddr string `json:"admin_addr"`
}

type command struct {
	Operation string    `json:"op"`
	Namespace string    `json:"namespace,omitempty"`
	Key       string    `json:"key,omitempty"`
	Value     []byte    `json:"value,omitempty"`
	Peer      *peerInfo `json:"peer,omitempty"`
	Commands  []command `json:"commands,omitempty"`
	Slot      uint32    `json:"slot,omitempty"`
	Sealed    bool      `json:"sealed,omitempty"`
}

type applyResult struct {
	Revision       uint64
	Revisions      []uint64
	Errors         []error
	BatchRevisions [][]uint64 // only populated for nested BatchWrite children
	Err            error
}

// FSM is the replicated Pebble state machine. All writes must come from Raft,
// never from calls to pebble.Store.Set or pebble.Store.Delete.
type FSM struct {
	mu           sync.RWMutex
	readMu       sync.RWMutex
	published    *pebble.Snapshot
	readFailure  error // guarded by readMu; GET never waits behind Pebble fsync
	db           *pebble.DB
	revision     uint64
	applied      uint64
	oldest       uint64
	hasFloor     bool
	historyBytes uint64
	notifier     changefeed.Notifier
	sealed       map[sealID]bool
	failed       error
	metrics      fsmApplyMetrics
}

// FSMPerformance separates Pebble's durable commit time from the rest of the
// Raft FSM apply path. All metrics are cumulative and read without locking.
type FSMPerformance struct {
	CommitBatches         uint64  `json:"commit_batches"`
	CommittedLogs         uint64  `json:"committed_raft_logs"`
	LogicalWrites         uint64  `json:"logical_writes"`
	AverageLogsPerBatch   float64 `json:"average_logs_per_commit"`
	AverageWritesPerBatch float64 `json:"average_writes_per_commit"`
	AveragePebbleCommitMS float64 `json:"average_pebble_commit_ms"`
	AverageFSMApplyMS     float64 `json:"average_fsm_apply_ms"`
	AverageLockWaitMS     float64 `json:"average_fsm_lock_wait_ms"`
}

type fsmApplyMetrics struct {
	commits     atomic.Uint64
	logs        atomic.Uint64
	writes      atomic.Uint64
	commitNanos atomic.Uint64
	applyNanos  atomic.Uint64
	lockNanos   atomic.Uint64
}

func (f *FSM) Performance() FSMPerformance {
	commits := f.metrics.commits.Load()
	result := FSMPerformance{CommitBatches: commits, CommittedLogs: f.metrics.logs.Load(), LogicalWrites: f.metrics.writes.Load()}
	if commits > 0 {
		result.AverageLogsPerBatch = float64(result.CommittedLogs) / float64(commits)
		result.AverageWritesPerBatch = float64(result.LogicalWrites) / float64(commits)
		result.AveragePebbleCommitMS = float64(f.metrics.commitNanos.Load()) / float64(commits) / 1e6
		result.AverageFSMApplyMS = float64(f.metrics.applyNanos.Load()) / float64(commits) / 1e6
		result.AverageLockWaitMS = float64(f.metrics.lockNanos.Load()) / float64(commits) / 1e6
	}
	return result
}

var _ raft.BatchingFSM = (*FSM)(nil)

func openFSM(path string) (*FSM, error) {
	db, err := pebble.Open(path, nil)
	if err != nil {
		return nil, fmt.Errorf("open replicated Pebble: %w", err)
	}
	f := &FSM{db: db}
	if err := f.refreshMeta(); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Publish only already-durable data. Raft Apply swaps this snapshot
	// after the synchronous Pebble commit has succeeded.
	f.published = db.NewSnapshot()
	return f, nil
}

func (f *FSM) refreshMeta() error {
	load := func(key []byte) (uint64, error) {
		raw, closer, err := f.db.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		defer closer.Close()
		if len(raw) != 8 {
			return 0, fmt.Errorf("invalid metadata at %x", key)
		}
		return binary.BigEndian.Uint64(raw), nil
	}
	rev, err := load(revisionKey)
	if err != nil {
		return err
	}
	applied, err := load(appliedKey)
	if err != nil {
		return err
	}
	f.revision, f.applied = rev, applied
	history, err := load(changefeed.ByteCountKey)
	if err != nil {
		return err
	}
	f.historyBytes = history
	floor, closer, err := f.db.Get(changefeed.FloorKey)
	if errors.Is(err, pebble.ErrNotFound) {
		f.hasFloor = false
		if rev == math.MaxUint64 {
			return errors.New("revision exhausted")
		}
		f.oldest = rev + 1 // legacy Raft state without change history
	} else if err != nil {
		return err
	} else {
		f.hasFloor = true
		decoded, e := changefeed.DecodeFloor(floor)
		_ = closer.Close()
		if e != nil {
			return e
		}
		f.oldest = decoded
	}
	if f.oldest > rev+1 {
		return errors.New("history floor exceeds revision")
	}
	sealed := make(map[sealID]bool)
	it, err := f.db.NewIter(&pebble.IterOptions{LowerBound: []byte{4}, UpperBound: []byte{5}})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		id, e := decodeSealedKey(it.Key())
		if e != nil {
			_ = it.Close()
			return e
		}
		if !bytes.Equal(it.Value(), []byte{1}) {
			_ = it.Close()
			return errors.New("invalid sealed slot value")
		}
		sealed[id] = true
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		return err
	}
	if err := it.Close(); err != nil {
		return err
	}
	f.sealed = sealed
	return nil
}

func (f *FSM) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readMu.Lock()
	if f.published != nil {
		_ = f.published.Close()
		f.published = nil
	}
	f.readMu.Unlock()
	return f.db.Close()
}
func (f *FSM) Failure() error {
	f.readMu.RLock()
	defer f.readMu.RUnlock()
	return f.readFailure
}

func encodeRecordKey(ns, key string) []byte {
	b := binary.AppendUvarint([]byte{1}, uint64(len(ns)))
	b = append(b, ns...)
	return append(b, key...)
}
func encodeRecord(value []byte, rev uint64) []byte {
	b := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(b[:8], rev)
	copy(b[8:], value)
	return b
}
func encode64(value uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], value)
	return b[:]
}
func peerKey(id string) []byte { return append([]byte{2}, id...) }

type sealID struct {
	namespace string
	slot      uint16
}

func sealedKey(namespace string, slot uint16) []byte {
	b := binary.AppendUvarint([]byte{4}, uint64(len(namespace)))
	b = append(b, namespace...)
	return binary.BigEndian.AppendUint16(b, slot)
}
func decodeSealedKey(b []byte) (sealID, error) {
	if len(b) < 4 || b[0] != 4 {
		return sealID{}, errors.New("invalid sealed key")
	}
	n, length := binary.Uvarint(b[1:])
	if length <= 0 || 1+length+2 > len(b) || n > uint64(len(b)-1-length-2) {
		return sealID{}, errors.New("invalid sealed namespace length")
	}
	ns := string(b[1+length : 1+length+int(n)])
	trailer := b[1+length+int(n):]
	if len(trailer) != 2 || ns == "" {
		return sealID{}, errors.New("invalid sealed slot metadata")
	}
	slot := binary.BigEndian.Uint16(trailer)
	if slot >= sharding.SlotCount {
		return sealID{}, errors.New("out-of-range sealed slot")
	}
	return sealID{ns, slot}, nil
}

func (f *FSM) Get(ctx context.Context, ns, key string) (storage.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return storage.Record{}, false, err
	}
	// The read snapshot is updated only AFTER the corresponding Raft apply
	// batch has completed a synchronous Pebble commit. A GET therefore never
	// waits for the writer's fsync and never sees uncommitted data.
	f.readMu.RLock()
	defer f.readMu.RUnlock()
	if f.readFailure != nil {
		return storage.Record{}, false, f.readFailure
	}
	if f.published == nil {
		return storage.Record{}, false, errors.New("cluster FSM closed")
	}
	raw, closer, err := f.published.Get(encodeRecordKey(ns, key))
	if errors.Is(err, pebble.ErrNotFound) {
		return storage.Record{}, false, nil
	}
	if err != nil {
		return storage.Record{}, false, fmt.Errorf("read record: %w", err)
	}
	defer closer.Close()
	if len(raw) < 8 {
		return storage.Record{}, false, errors.New("corrupt replicated record")
	}
	return storage.Record{Revision: binary.BigEndian.Uint64(raw[:8]), Value: bytes.Clone(raw[8:])}, true, nil
}

func (f *FSM) Peer(id string) (peerInfo, bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	raw, closer, err := f.db.Get(peerKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return peerInfo{}, false, nil
	}
	if err != nil {
		return peerInfo{}, false, err
	}
	defer closer.Close()
	var peer peerInfo
	if err := json.Unmarshal(raw, &peer); err != nil {
		return peerInfo{}, false, err
	}
	return peer, true, nil
}

// Apply delegates to the same atomic implementation used by ApplyBatch.
func (f *FSM) Apply(log *raft.Log) any {
	return f.ApplyBatch([]*raft.Log{log})[0]
}

// ApplyBatch is called with committed, ordered Raft logs. Every batch (up to
// MaxAppendEntries) becomes ONE durable Pebble commit, including revision,
// changefeed events, peer routing metadata, and applied Raft index.
// A replayed index is ignored. Configuration logs advance the applied index
// without generating a KV revision.
func (f *FSM) ApplyBatch(logs []*raft.Log) []interface{} {
	responses := make([]interface{}, len(logs))
	for i := range responses {
		responses[i] = applyResult{}
	}
	if len(logs) == 0 {
		return responses
	}
	started := time.Now()
	f.mu.Lock()
	lockWait := time.Since(started)
	defer f.mu.Unlock()
	fail := func(err error) []interface{} {
		f.failed = err
		f.readMu.Lock()
		f.readFailure = err
		f.readMu.Unlock()
		for i := range responses {
			responses[i] = applyResult{Err: err}
		}
		return responses
	}
	if f.failed != nil {
		return fail(f.failed)
	}

	batch := f.db.NewBatch()
	defer batch.Close()
	history := changefeed.NewBatchWriter(f.db, batch, f.oldest, f.historyBytes, f.hasFloor)
	shadow := make(map[string]bool, len(logs))
	stagedSeals := make(map[sealID]bool)
	isSealed := func(c command) bool {
		if len(f.sealed) == 0 && len(stagedSeals) == 0 {
			return false
		}
		id := sealID{c.Namespace, sharding.Slot(c.Namespace, c.Key)}
		if value, found := stagedSeals[id]; found {
			return value
		}
		return f.sealed[id]
	}
	nextRevision, nextApplied := f.revision, f.applied
	changed := false

	// Process each logical write in Raft log order, including multiple
	// commands packed into one log. A same-key delete must see earlier
	// uncommitted sets/deletes from the same Pebble batch.
	applyOne := func(cmd command) (uint64, error) {
		revision := uint64(0)
		switch cmd.Operation {
		case opSet, opDelete:
			encoded := encodeRecordKey(cmd.Namespace, cmd.Key)
			key := string(encoded)
			if cmd.Operation == opDelete {
				exists, known := shadow[key]
				if !known {
					_, closer, err := f.db.Get(encoded)
					switch {
					case errors.Is(err, pebble.ErrNotFound):
						exists = false
					case err != nil:
						return 0, fmt.Errorf("read before delete: %w", err)
					default:
						exists = true
						if err := closer.Close(); err != nil {
							return 0, err
						}
					}
				}
				if !exists {
					return 0, nil
				}
			}
			if nextRevision == math.MaxUint64 {
				return 0, errors.New("revision exhausted")
			}
			nextRevision++
			revision = nextRevision
			var err error
			if cmd.Operation == opSet {
				err = batch.Set(encoded, encodeRecord(cmd.Value, revision), nil)
				shadow[key] = true
			} else {
				err = batch.Delete(encoded, nil)
				shadow[key] = false
			}
			if err != nil {
				return 0, err
			}
			err = history.Append(changefeed.Change{Revision: revision, Namespace: cmd.Namespace, Key: cmd.Key, Value: cmd.Value, Deleted: cmd.Operation == opDelete})
			if err != nil {
				return 0, err
			}
			changed = true
		case opFence:
			if cmd.Namespace == "" || cmd.Slot >= sharding.SlotCount {
				return 0, errors.New("invalid migration fence")
			}
			id := sealID{cmd.Namespace, uint16(cmd.Slot)}
			if cmd.Sealed {
				if err := batch.Set(sealedKey(cmd.Namespace, id.slot), []byte{1}, nil); err != nil {
					return 0, err
				}
			} else {
				if err := batch.Delete(sealedKey(cmd.Namespace, id.slot), nil); err != nil {
					return 0, err
				}
			}
			stagedSeals[id] = cmd.Sealed
		case opPeer:
			if cmd.Peer == nil || cmd.Peer.ID == "" || cmd.Peer.RaftAddr == "" || cmd.Peer.GRPCAddr == "" || cmd.Peer.AdminAddr == "" {
				return 0, errors.New("invalid peer metadata")
			}
			raw, err := json.Marshal(cmd.Peer)
			if err != nil {
				return 0, err
			}
			if err := batch.Set(peerKey(cmd.Peer.ID), raw, nil); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("unrecognized committed operation %q", cmd.Operation)
		}
		return revision, nil
	}

	for i, log := range logs {
		if log.Index <= nextApplied {
			continue
		} // durable index already applied
		if log.Type != raft.LogCommand && log.Type != raft.LogConfiguration {
			return fail(fmt.Errorf("unexpected committed Raft log type %v at index %d", log.Type, log.Index))
		}
		if log.Type == raft.LogConfiguration {
			nextApplied = log.Index
			continue
		}
		var cmd command
		if err := json.Unmarshal(log.Data, &cmd); err != nil {
			return fail(fmt.Errorf("decode committed log %d: %w", log.Index, err))
		}
		if cmd.Operation == opMicrobatch {
			if len(cmd.Commands) < 1 || len(cmd.Commands) > maxRaftCommandsPerBatch {
				return fail(errors.New("invalid microbatch size"))
			}
			logicalCount := 0
			// Validate the complete envelope before staging any writes. Nested
			// opBatch children are indivisible RPC transactions even when sharing
			// a physical Raft log with unrelated clients.
			for _, item := range cmd.Commands {
				switch item.Operation {
				case opSet, opDelete:
					logicalCount++
				case opBatch:
					if len(item.Commands) < 1 || len(item.Commands) > 128 {
						return fail(errors.New("invalid nested client batch size"))
					}
					logicalCount += len(item.Commands)
					for _, nested := range item.Commands {
						if nested.Operation != opSet && nested.Operation != opDelete {
							return fail(fmt.Errorf("invalid nested batch operation %q", nested.Operation))
						}
					}
				default:
					return fail(fmt.Errorf("invalid microbatch operation %q", item.Operation))
				}
			}
			if logicalCount > maxRaftCommandsPerBatch {
				return fail(errors.New("too many logical writes in microbatch"))
			}
			revisions := make([]uint64, len(cmd.Commands))
			itemErrors := make([]error, len(cmd.Commands))
			batchRevisions := make([][]uint64, len(cmd.Commands))
			for j, item := range cmd.Commands {
				if item.Operation == opBatch {
					blocked := false
					for _, nested := range item.Commands {
						if isSealed(nested) {
							blocked = true
							break
						}
					}
					if blocked {
						itemErrors[j] = status.Error(codes.FailedPrecondition, "write rejected by replicated migration fence; refresh routing")
						continue
					}
					batchRevisions[j] = make([]uint64, len(item.Commands))
					for k, nested := range item.Commands {
						rev, err := applyOne(nested)
						if err != nil {
							return fail(err)
						}
						batchRevisions[j][k] = rev
					}
					continue
				}
				if isSealed(item) {
					itemErrors[j] = status.Error(codes.FailedPrecondition, "write rejected by replicated migration fence; refresh routing")
					continue
				}
				rev, err := applyOne(item)
				if err != nil {
					return fail(err)
				}
				revisions[j] = rev
			}
			responses[i] = applyResult{Revisions: revisions, Errors: itemErrors, BatchRevisions: batchRevisions}
		} else if cmd.Operation == opBatch {
			if len(cmd.Commands) == 0 || len(cmd.Commands) > maxRaftCommandsPerBatch {
				return fail(errors.New("invalid raft command batch size"))
			}
			revisions := make([]uint64, len(cmd.Commands))
			blocked := false
			for _, item := range cmd.Commands {
				if item.Operation != opSet && item.Operation != opDelete {
					return fail(fmt.Errorf("invalid batched operation %q", item.Operation))
				}
				if isSealed(item) {
					blocked = true
				}
			}
			if blocked {
				responses[i] = applyResult{Err: status.Error(codes.FailedPrecondition, "write rejected by replicated migration fence; refresh routing")}
				nextApplied = log.Index
				continue
			}
			for j, item := range cmd.Commands {
				if item.Operation != opSet && item.Operation != opDelete {
					return fail(fmt.Errorf("invalid batched operation %q", item.Operation))
				}
				rev, err := applyOne(item)
				if err != nil {
					return fail(err)
				}
				revisions[j] = rev
			}
			responses[i] = applyResult{Revisions: revisions}
		} else {
			if (cmd.Operation == opSet || cmd.Operation == opDelete) && isSealed(cmd) {
				responses[i] = applyResult{Err: status.Error(codes.FailedPrecondition, "write rejected by replicated migration fence; refresh routing")}
				nextApplied = log.Index
				continue
			}
			revision, err := applyOne(cmd)
			if err != nil {
				return fail(err)
			}
			responses[i] = applyResult{Revision: revision}
		}
		nextApplied = log.Index
	}
	if nextApplied == f.applied {
		return responses
	}
	if changed {
		if err := history.Flush(); err != nil {
			return fail(err)
		}
		if err := batch.Set(revisionKey, encode64(nextRevision), nil); err != nil {
			return fail(err)
		}
	}
	if err := batch.Set(appliedKey, encode64(nextApplied), nil); err != nil {
		return fail(err)
	}
	commitStarted := time.Now()
	if err := batch.Commit(pebble.Sync); err != nil {
		return fail(fmt.Errorf("apply committed Raft batch ending at %d: %w", nextApplied, err))
	}
	commitDuration := time.Since(commitStarted)
	logicalWrites := nextRevision - f.revision
	f.applied = nextApplied
	for id, sealed := range stagedSeals {
		if sealed {
			f.sealed[id] = true
		} else {
			delete(f.sealed, id)
		}
	}
	if changed {
		// Do not publish until Pebble.Sync is complete. Acquire readMu only
		// for the snapshot swap; outstanding readers finish before old.Close.
		newRead := f.db.NewSnapshot()
		f.readMu.Lock()
		oldRead := f.published
		f.published = newRead
		f.readMu.Unlock()
		// The write lock guarantees that no previous reader still uses
		// oldRead. Close it OUTSIDE the read lock to minimize GET tail latency.
		if oldRead != nil {
			_ = oldRead.Close()
		}
		f.revision = nextRevision
		f.oldest, f.historyBytes = history.State()
		f.hasFloor = true
		f.notifier.Signal()
	}
	f.metrics.commitNanos.Add(uint64(commitDuration.Nanoseconds()))
	f.metrics.lockNanos.Add(uint64(lockWait.Nanoseconds()))
	f.metrics.logs.Add(uint64(len(logs)))
	f.metrics.writes.Add(logicalWrites)
	f.metrics.applyNanos.Add(uint64(time.Since(started).Nanoseconds()))
	f.metrics.commits.Add(1)
	return responses
}

// Snapshot captures a consistent Pebble snapshot quickly. The potentially
// expensive streaming runs in FSMSnapshot.Persist, outside the Raft apply loop.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.failed != nil {
		return nil, f.failed
	}
	return &fsmSnapshot{snapshot: f.db.NewSnapshot()}, nil
}

type fsmSnapshot struct{ snapshot *pebble.Snapshot }

func (s *fsmSnapshot) Release() { _ = s.snapshot.Close() }
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer func() {
		if err != nil {
			_ = sink.Cancel()
		}
	}()
	if _, err = io.WriteString(sink, snapshotMagic); err != nil {
		return err
	}
	it, err := s.snapshot.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()
	var hdr [8]byte
	for it.First(); it.Valid(); it.Next() {
		key, value := it.Key(), it.Value()
		if len(key) == 0 || len(key) > maxSnapshotField || len(value) > maxSnapshotField {
			return errors.New("snapshot entry too large")
		}
		binary.BigEndian.PutUint32(hdr[:4], uint32(len(key)))
		binary.BigEndian.PutUint32(hdr[4:], uint32(len(value)))
		if _, err = sink.Write(hdr[:]); err != nil {
			return err
		}
		if _, err = sink.Write(key); err != nil {
			return err
		}
		if _, err = sink.Write(value); err != nil {
			return err
		}
	}
	if err = it.Error(); err != nil {
		return err
	}
	// 0-length key marks end of snapshot.
	if _, err = sink.Write(make([]byte, 8)); err != nil {
		return err
	}
	return sink.Close()
}

// Restore replaces the Pebble contents atomically. A single Pebble batch is
// used for this first version (max Pebble batch ~4GiB); large clusters should
// use staged DB restore / SST ingestion before deploying huge datasets.
func (f *FSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return err
	}
	if string(magic) != snapshotMagic {
		return errors.New("unrecognized snapshot format")
	}
	batch := f.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange([]byte{0}, []byte{255}, nil); err != nil {
		return err
	}
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(reader, hdr[:]); err != nil {
			return err
		}
		keySize := binary.BigEndian.Uint32(hdr[:4])
		valSize := binary.BigEndian.Uint32(hdr[4:])
		if keySize == 0 && valSize == 0 {
			break
		}
		if keySize == 0 || keySize > maxSnapshotField || valSize > maxSnapshotField {
			return errors.New("corrupt snapshot entry length")
		}
		key := make([]byte, int(keySize))
		value := make([]byte, int(valSize))
		if _, err := io.ReadFull(reader, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return err
		}
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	f.failed = nil
	if err := f.refreshMeta(); err != nil {
		f.failed = err
		f.readMu.Lock()
		f.readFailure = err
		f.readMu.Unlock()
		return err
	}
	newRead := f.db.NewSnapshot()
	f.readMu.Lock()
	oldRead := f.published
	f.published = newRead
	f.readFailure = nil
	f.readMu.Unlock()
	if oldRead != nil {
		_ = oldRead.Close()
	}
	f.notifier.Signal()
	return nil
}

func (f *FSM) ReadChanges(ctx context.Context, after uint64, limit int) (changefeed.Page, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.failed != nil {
		return changefeed.Page{}, f.failed
	}
	return changefeed.Read(ctx, f.db, after, f.revision, f.oldest, limit)
}
func (f *FSM) Notify() <-chan struct{} { return f.notifier.Subscribe() }

func (f *FSM) Head(ctx context.Context) (uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.failed != nil {
		return 0, 0, f.failed
	}
	return f.revision, f.oldest, nil
}

func (f *FSM) ScanNamespace(ctx context.Context, namespace string, emit func(storage.ScanItem) error) error {
	f.mu.RLock()
	if f.failed != nil {
		err := f.failed
		f.mu.RUnlock()
		return err
	}
	snapshot := f.db.NewSnapshot()
	head := f.revision
	f.mu.RUnlock()
	defer snapshot.Close()
	return changefeed.ScanSnapshot(ctx, snapshot, head, namespace, emit)
}
