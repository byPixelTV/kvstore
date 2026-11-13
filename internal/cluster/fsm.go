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

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/cockroachdb/pebble/v2"
	"github.com/hashicorp/raft"
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
}

type applyResult struct {
	Revision uint64
	Err      error
}

// FSM is the replicated Pebble state machine. All writes must come from Raft,
// never from calls to pebble.Store.Set or pebble.Store.Delete.
type FSM struct {
	mu           sync.RWMutex
	db           *pebble.DB
	revision     uint64
	applied      uint64
	oldest       uint64
	hasFloor     bool
	historyBytes uint64
	notifier     changefeed.Notifier
	failed       error
}

var _ raft.FSM = (*FSM)(nil)

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
	return nil
}

func (f *FSM) Close() error   { f.mu.Lock(); defer f.mu.Unlock(); return f.db.Close() }
func (f *FSM) Failure() error { f.mu.RLock(); defer f.mu.RUnlock(); return f.failed }

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

func (f *FSM) Get(ctx context.Context, ns, key string) (storage.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return storage.Record{}, false, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.failed != nil {
		return storage.Record{}, false, f.failed
	}
	raw, closer, err := f.db.Get(encodeRecordKey(ns, key))
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

func (f *FSM) Apply(log *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed != nil {
		return applyResult{Err: f.failed}
	}
	// A committed log may be replayed after restarting while Pebble still
	// contains its effects. The applied index is persisted in the same batch.
	if log.Index <= f.applied {
		return applyResult{}
	}
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.failed = fmt.Errorf("decode committed log %d: %w", log.Index, err)
		return applyResult{Err: f.failed}
	}
	batch := f.db.NewBatch()
	defer batch.Close()
	rev := uint64(0)
	var err error
	switch cmd.Operation {
	case opSet:
		if f.revision == math.MaxUint64 {
			err = errors.New("revision exhausted")
			break
		}
		rev = f.revision + 1
		err = batch.Set(encodeRecordKey(cmd.Namespace, cmd.Key), encodeRecord(cmd.Value, rev), nil)
	case opDelete:
		encoded := encodeRecordKey(cmd.Namespace, cmd.Key)
		_, closer, getErr := f.db.Get(encoded)
		if errors.Is(getErr, pebble.ErrNotFound) {
			break
		}
		if getErr != nil {
			err = getErr
			break
		}
		if closeErr := closer.Close(); closeErr != nil {
			err = closeErr
			break
		}
		if f.revision == math.MaxUint64 {
			err = errors.New("revision exhausted")
			break
		}
		rev = f.revision + 1
		err = batch.Delete(encoded, nil)
	case opPeer:
		if cmd.Peer == nil || cmd.Peer.ID == "" || cmd.Peer.RaftAddr == "" || cmd.Peer.GRPCAddr == "" || cmd.Peer.AdminAddr == "" {
			err = errors.New("invalid peer metadata")
			break
		}
		var raw []byte
		raw, err = json.Marshal(cmd.Peer)
		if err == nil {
			err = batch.Set(peerKey(cmd.Peer.ID), raw, nil)
		}
	default:
		err = fmt.Errorf("unrecognized committed operation %q", cmd.Operation)
	}
	newFloor, newHistory := f.oldest, f.historyBytes
	if err == nil && rev != 0 {
		newFloor, newHistory, err = changefeed.Append(f.db, batch, changefeed.Change{Revision: rev, Namespace: cmd.Namespace, Key: cmd.Key, Value: cmd.Value, Deleted: cmd.Operation == opDelete}, f.oldest, f.historyBytes, f.hasFloor)
		if err == nil {
			err = batch.Set(revisionKey, encode64(rev), nil)
		}
	}
	if err == nil {
		err = batch.Set(appliedKey, encode64(log.Index), nil)
	}
	if err == nil {
		err = batch.Commit(pebble.Sync)
	}
	if err != nil {
		f.failed = fmt.Errorf("apply committed raft index %d: %w", log.Index, err)
		return applyResult{Err: f.failed}
	}
	if rev != 0 {
		f.oldest = newFloor
		f.historyBytes = newHistory
		f.hasFloor = true
		f.revision = rev
		f.notifier.Signal()
	}
	f.applied = log.Index
	return applyResult{Revision: rev}
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
		return err
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
