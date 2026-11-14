package sharding

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type memRecord struct {
	value    []byte
	revision uint64
}
type memShard struct {
	kvstorev1.KVStoreClient
	mu       sync.Mutex
	revision uint64
	data     map[string]memRecord
	changes  []*kvstorev1.WatchResponse
	sealed   map[slotKey]bool
	onScan   func()
}

func newMemShard() *memShard {
	return &memShard{data: map[string]memRecord{}, sealed: map[slotKey]bool{}}
}
func memKey(ns, key string) string { return ns + "\x00" + key }
func (s *memShard) Get(_ context.Context, req *kvstorev1.GetRequest, _ ...grpc.CallOption) (*kvstorev1.GetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, found := s.data[memKey(req.GetNamespace(), req.GetKey())]
	return &kvstorev1.GetResponse{Found: found, Value: append([]byte(nil), item.value...), Revision: item.revision}, nil
}
func (s *memShard) Set(_ context.Context, req *kvstorev1.SetRequest, _ ...grpc.CallOption) (*kvstorev1.SetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed[slotKey{req.GetNamespace(), Slot(req.GetNamespace(), req.GetKey())}] {
		return nil, status.Error(codes.FailedPrecondition, "sealed")
	}
	s.revision++
	s.data[memKey(req.GetNamespace(), req.GetKey())] = memRecord{append([]byte(nil), req.GetValue()...), s.revision}
	s.changes = append(s.changes, &kvstorev1.WatchResponse{Revision: s.revision, Namespace: req.GetNamespace(), Key: req.GetKey(), Value: append([]byte(nil), req.GetValue()...)})
	return &kvstorev1.SetResponse{Revision: s.revision}, nil
}
func (s *memShard) Delete(_ context.Context, req *kvstorev1.DeleteRequest, _ ...grpc.CallOption) (*kvstorev1.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed[slotKey{req.GetNamespace(), Slot(req.GetNamespace(), req.GetKey())}] {
		return nil, status.Error(codes.FailedPrecondition, "sealed")
	}
	key := memKey(req.GetNamespace(), req.GetKey())
	if _, found := s.data[key]; !found {
		return &kvstorev1.DeleteResponse{}, nil
	}
	delete(s.data, key)
	s.revision++
	s.changes = append(s.changes, &kvstorev1.WatchResponse{Revision: s.revision, Namespace: req.GetNamespace(), Key: req.GetKey(), Deleted: true})
	return &kvstorev1.DeleteResponse{Deleted: true, Revision: s.revision}, nil
}
func (s *memShard) Head(context.Context, *kvstorev1.HeadRequest, ...grpc.CallOption) (*kvstorev1.HeadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &kvstorev1.HeadResponse{Revision: s.revision}, nil
}
func (s *memShard) FenceSlot(_ context.Context, req *kvstorev1.FenceSlotRequest, _ ...grpc.CallOption) (*kvstorev1.FenceSlotResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sealed[slotKey{req.GetNamespace(), uint16(req.GetSlot())}] = req.GetSealed()
	return &kvstorev1.FenceSlotResponse{}, nil
}
func (s *memShard) Scan(_ context.Context, req *kvstorev1.ScanRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[kvstorev1.ScanResponse], error) {
	s.mu.Lock()
	items := []*kvstorev1.ScanResponse{{Checkpoint: true, SnapshotRevision: s.revision}}
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		if strings.HasPrefix(key, req.GetNamespace()+"\x00") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		item := s.data[k]
		items = append(items, &kvstorev1.ScanResponse{Key: strings.TrimPrefix(k, req.GetNamespace()+"\x00"), Value: append([]byte(nil), item.value...), Revision: item.revision})
	}
	hook := s.onScan
	s.onScan = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	} // simulate concurrent committed source write after snapshot
	return &fakeScanStream{items: items}, nil
}

type memoryWatch struct {
	grpc.ClientStream
	index  int
	events []*kvstorev1.WatchResponse
	head   uint64
}

func (s *memoryWatch) Recv() (*kvstorev1.WatchResponse, error) {
	if s.index < len(s.events) {
		e := s.events[s.index]
		s.index++
		return e, nil
	}
	return &kvstorev1.WatchResponse{Checkpoint: true, Revision: s.head}, nil
}
func (s *memShard) Watch(_ context.Context, req *kvstorev1.WatchRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[kvstorev1.WatchResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []*kvstorev1.WatchResponse
	for _, e := range s.changes {
		if e.GetRevision() > req.GetAfterRevision() && e.GetNamespace() == req.GetNamespace() {
			events = append(events, e)
		}
	}
	return &memoryWatch{events: events, head: s.revision}, nil
}
func newMigrationRouter() (*Router, [ShardCount]*memShard) {
	r := &Router{clusterFingerprint: "test-fixture", fenceToken: "test-join-secret"}
	r.routing.Store(&routingState{Version: 1, Epoch: 1, Fingerprint: r.clusterFingerprint, Overrides: map[string]map[uint16]uint8{}})
	r.loaded.Store(true)
	var shards [ShardCount]*memShard
	for id := range shards {
		shards[id] = newMemShard()
		g := &group{}
		g.clients[0] = []kvstorev1.KVStoreClient{shards[id]}
		g.leader.Store(0)
		r.groups[id] = g
	}
	return r, shards
}
func matchingKeys(ns string, slot uint16, quantity int) []string {
	var keys []string
	for i := 0; len(keys) < quantity; i++ {
		key := fmt.Sprintf("player-%d", i)
		if Slot(ns, key) == slot {
			keys = append(keys, key)
		}
	}
	return keys
}
func TestOnlineMigrationCopyReplayCutoverAndRecovery(t *testing.T) {
	ctx := context.Background()
	r, shards := newMigrationRouter()
	ns := "players"
	slot := Slot(ns, "player:42")
	src := int(slot) % ShardCount
	target := (src + 1) % ShardCount
	keys := matchingKeys(ns, slot, 3)
	for _, key := range keys {
		if _, err := shards[src].Set(ctx, &kvstorev1.SetRequest{Namespace: ns, Key: key, Value: []byte("old")}); err != nil {
			t.Fatal(err)
		}
	}
	// A stale copy left by an earlier aborted transfer must disappear.
	_, _ = shards[target].Set(ctx, &kvstorev1.SetRequest{Namespace: ns, Key: matchingKeys(ns, slot, 4)[3], Value: []byte("stale")})
	// Simulate a write after the Scan's snapshot but before replay.
	shards[src].onScan = func() {
		_, _ = shards[src].Set(ctx, &kvstorev1.SetRequest{Namespace: ns, Key: keys[0], Value: []byte("new")})
	}
	oldCursor := encodeEpochCursor(Cursor{1, 1, 1}, 1)
	report, err := r.MigrateSlot(ctx, ns, slot, target)
	if err != nil {
		t.Fatal(err)
	}
	if report.Source != src || report.Target != target || report.Copied != 3 || report.Replayed < 1 || report.NewEpoch != 2 {
		t.Fatalf("bad migration stats: %+v", report)
	}
	for i, key := range keys {
		got, err := r.Get(ctx, &kvstorev1.GetRequest{Namespace: ns, Key: key})
		if err != nil || !got.GetFound() {
			t.Fatalf("missing migrated key %q %v", key, err)
		}
		want := "old"
		if i == 0 {
			want = "new"
		}
		if string(got.GetValue()) != want || got.GetRevision()>>56 != uint64(target+1) {
			t.Fatalf("wrong migrated %s: %+v", key, got)
		}
	}
	if _, err := shards[src].Set(ctx, &kvstorev1.SetRequest{Namespace: ns, Key: keys[0], Value: []byte("late")}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old owner still accepts writes: %v", err)
	}
	if _, err := r.Set(ctx, &kvstorev1.SetRequest{Namespace: ns, Key: keys[0], Value: []byte("after")}); err != nil {
		t.Fatalf("new owner did not accept write: %v", err)
	}
	if err := r.Watch(&kvstorev1.WatchRequest{Namespace: ns, UseVectorCursor: true, Cursor: oldCursor}, &watchSink{ctx: ctx}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale cursor accepted after epoch change: %v", err)
	}
	// Simulate router restart: load persisted mapping from shard 0.
	r2 := &Router{clusterFingerprint: r.clusterFingerprint, fenceToken: r.fenceToken}
	for i, g := range r.groups {
		r2.groups[i] = g
	}
	if err := r2.LoadRouting(ctx); err != nil {
		t.Fatal(err)
	}
	if r2.current().owner(ns, slot) != target {
		t.Fatal("restart lost durable slot owner")
	}
	// Migration back to original group must unseal the old owner's slot.
	back, err := r.MigrateSlot(ctx, ns, slot, src)
	if err != nil {
		t.Fatal(err)
	}
	if back.NewEpoch != 3 || r.current().owner(ns, slot) != src {
		t.Fatalf("reverse migration failed: %+v", back)
	}
}
func TestRoutingEpochCursorFailClosed(t *testing.T) {
	c := Cursor{1, 2, 3}
	for _, epoch := range []uint64{1, 2, 500} {
		raw := encodeEpochCursor(c, epoch)
		parsed, err := decodeEpochCursor(raw, epoch)
		if err != nil || parsed != c {
			t.Fatalf("decode epoch %d: %v, %v", epoch, parsed, err)
		}
		_, err = decodeEpochCursor(raw, epoch+1)
		if err == nil {
			t.Fatalf("stale epoch accepted for %d", epoch)
		}
	}
	if _, err := decodeEpochCursor(c.Encode(), 2); err == nil {
		t.Fatal("legacy cursor accepted after migration")
	}
}
func TestRoutingStateCorruptionRejected(t *testing.T) {
	r, _ := newMigrationRouter()
	state := r.current().copy()
	state.Overrides["n"] = map[uint16]uint8{100: 4}
	if err := state.validate(r.clusterFingerprint); err == nil {
		t.Fatal("invalid shard accepted")
	}
	state.Overrides["n"] = map[uint16]uint8{100: 1}
	raw, _ := json.Marshal(state)
	var roundTrip routingState
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if err := roundTrip.validate(r.clusterFingerprint); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeEpochCursor("not-a-cursor", 1); !errors.Is(err, io.EOF) && err == nil {
		t.Fatal("invalid cursor accepted")
	}
}

// A quiet source must not wait for the Watch stream's 20-second heartbeat.
// The checkpoint already proves that the requested target revision is reached.
type noReceiveWatch struct{ grpc.ClientStream }

func (s *noReceiveWatch) Recv() (*kvstorev1.WatchResponse, error) {
	panic("unnecessary Watch.Recv on an already caught-up migration")
}
func TestCatchUpWithNoNewChangesReturnsImmediately(t *testing.T) {
	cursor := uint64(42)
	report := &MigrationReport{}
	err := catchUp(context.Background(), &noReceiveWatch{}, newMemShard(), "players", 17, &cursor, 42, report)
	if err != nil || cursor != 42 || report.Replayed != 0 {
		t.Fatalf("catch-up on quiet shard: cursor=%d report=%+v err=%v", cursor, report, err)
	}
}
