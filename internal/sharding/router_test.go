package sharding

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeShard struct {
	kvstorev1.KVStoreClient
	id            int
	writes        int
	deletes       int
	failSet       bool
	batchAttempts int
}

func (f *fakeShard) Get(_ context.Context, _ *kvstorev1.GetRequest, _ ...grpc.CallOption) (*kvstorev1.GetResponse, error) {
	return &kvstorev1.GetResponse{Found: true, Value: []byte{byte(f.id)}, Revision: 5}, nil
}
func (f *fakeShard) Set(_ context.Context, _ *kvstorev1.SetRequest, _ ...grpc.CallOption) (*kvstorev1.SetResponse, error) {
	f.writes++
	if f.failSet {
		return nil, status.Error(codes.Unavailable, "uncertain write")
	}
	return &kvstorev1.SetResponse{Revision: 6}, nil
}
func (f *fakeShard) Delete(_ context.Context, _ *kvstorev1.DeleteRequest, _ ...grpc.CallOption) (*kvstorev1.DeleteResponse, error) {
	f.deletes++
	return &kvstorev1.DeleteResponse{Deleted: true, Revision: 7}, nil
}
func (f *fakeShard) Head(_ context.Context, _ *kvstorev1.HeadRequest, _ ...grpc.CallOption) (*kvstorev1.HeadResponse, error) {
	return &kvstorev1.HeadResponse{Revision: uint64(f.id + 10)}, nil
}
func newFakeRouter() (*Router, [ShardCount]*fakeShard) {
	r := &Router{}
	var mocks [ShardCount]*fakeShard
	for i := range mocks {
		mocks[i] = &fakeShard{id: i}
		g := &group{}
		g.clients[0] = []kvstorev1.KVStoreClient{mocks[i]}
		g.leader.Store(0)
		r.groups[i] = g
	}
	return r, mocks
}
func TestRouterPointRoutingAndRevisions(t *testing.T) {
	r, mocks := newFakeRouter()
	for i := 0; i < ShardCount; i++ {
		key := ""
		for k := 0; k < 1000; k++ {
			candidate := fmt.Sprintf("key-%d", k)
			if Shard("mc", candidate) == i {
				key = candidate
				break
			}
		}
		if key == "" {
			t.Fatal("missing key for shard")
		}
		set, err := r.Set(context.Background(), &kvstorev1.SetRequest{Namespace: "mc", Key: key, Value: []byte("value")})
		if err != nil || set.GetRevision()>>56 != uint64(i+1) {
			t.Fatalf("shard %d set rev=%v err=%v", i, set, err)
		}
		got, err := r.Get(context.Background(), &kvstorev1.GetRequest{Namespace: "mc", Key: key})
		if err != nil || !got.GetFound() || got.GetRevision()>>56 != uint64(i+1) || len(got.GetValue()) != 1 || int(got.GetValue()[0]) != i {
			t.Fatalf("shard %d wrong GET: %v %v", i, got, err)
		}
		del, err := r.Delete(context.Background(), &kvstorev1.DeleteRequest{Namespace: "mc", Key: key})
		if err != nil || !del.GetDeleted() || del.GetRevision()>>56 != uint64(i+1) {
			t.Fatalf("bad DELETE shard %d: %v %v", i, del, err)
		}
	}
	for i, m := range mocks {
		if m.writes != 1 || m.deletes != 1 {
			t.Fatalf("shard %d writes=%d deletes=%d", i, m.writes, m.deletes)
		}
	}
}
func TestNoAmbiguousWriteRetry(t *testing.T) {
	r, mocks := newFakeRouter()
	key := "abc"
	which := Shard("ns", key)
	mocks[which].failSet = true
	_, err := r.Set(context.Background(), &kvstorev1.SetRequest{Namespace: "ns", Key: key})
	if status.Code(err) != codes.Unavailable || mocks[which].writes != 1 {
		t.Fatalf("write retried or error lost: writes=%d err=%v", mocks[which].writes, err)
	}
	if r.groups[which].leader.Load() != -1 {
		t.Fatal("stale leader not marked unavailable")
	}
}
func TestLegacyCursorsFailClosed(t *testing.T) {
	r, _ := newFakeRouter()
	_, err := r.Head(context.Background(), &kvstorev1.HeadRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old Head accepted: %v", err)
	}
	_, err = r.Head(context.Background(), &kvstorev1.HeadRequest{UseVectorCursor: true})
	if err != nil {
		t.Fatal(err)
	}
	stream := &scanSink{ctx: context.Background()}
	if err := r.Scan(&kvstorev1.ScanRequest{Namespace: "mc"}, stream); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old Scan accepted: %v", err)
	}
	if err := r.Watch(&kvstorev1.WatchRequest{Namespace: "mc"}, &watchSink{ctx: context.Background()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old Watch accepted: %v", err)
	}
}
func TestVectorHead(t *testing.T) {
	r, _ := newFakeRouter()
	result, err := r.Head(context.Background(), &kvstorev1.HeadRequest{UseVectorCursor: true})
	if err != nil {
		t.Fatal(err)
	}
	c, err := DecodeCursor(result.GetCursor())
	if err != nil || c != (Cursor{10, 11, 12}) {
		t.Fatalf("unexpected vector head %v %v", c, err)
	}
	if result.GetRevision() != 0 {
		t.Fatal("must never fake a global revision")
	}
}

type fakeScanStream struct {
	grpc.ClientStream
	items []*kvstorev1.ScanResponse
	index int
}

func (s *fakeScanStream) Recv() (*kvstorev1.ScanResponse, error) {
	if s.index >= len(s.items) {
		return nil, io.EOF
	}
	v := s.items[s.index]
	s.index++
	return v, nil
}
func (f *fakeShard) Scan(_ context.Context, _ *kvstorev1.ScanRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[kvstorev1.ScanResponse], error) {
	key := fmt.Sprintf("key%d", f.id)
	for i := 0; Shard("mc", key) != f.id && i < 10000; i++ {
		key = fmt.Sprintf("key-%d-%d", f.id, i)
	}
	return &fakeScanStream{items: []*kvstorev1.ScanResponse{{Checkpoint: true, SnapshotRevision: uint64(100 + f.id)}, {Key: key, Value: []byte("abc"), Revision: 9}}}, nil
}

type scanSink struct {
	grpc.ServerStream
	ctx     context.Context
	results []*kvstorev1.ScanResponse
}

func (s *scanSink) Context() context.Context { return s.ctx }
func (s *scanSink) Send(r *kvstorev1.ScanResponse) error {
	s.results = append(s.results, r)
	return nil
}
func (s *scanSink) SetHeader(metadata.MD) error  { return nil }
func (s *scanSink) SendHeader(metadata.MD) error { return nil }
func (s *scanSink) SetTrailer(metadata.MD)       {}
func TestScanHasFinalVectorCheckpoint(t *testing.T) {
	r, _ := newFakeRouter()
	sink := &scanSink{ctx: context.Background()}
	if err := r.Scan(&kvstorev1.ScanRequest{Namespace: "mc", UseVectorCursor: true}, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.results) != 4 {
		t.Fatalf("got %d items, wanted 3 records + checkpoint", len(sink.results))
	}
	for i, item := range sink.results[:3] {
		if item.GetCheckpoint() || int(item.GetShard()) != i || item.GetRevision()>>56 != uint64(i+1) {
			t.Fatalf("shard %d item %v", i, item)
		}
	}
	final := sink.results[3]
	if !final.GetCheckpoint() {
		t.Fatal("missing final checkpoint")
	}
	c, err := DecodeCursor(final.GetCursor())
	if err != nil || c != (Cursor{100, 101, 102}) {
		t.Fatalf("wrong scan vector %v %v", c, err)
	}
}

type fakeWatchStream struct {
	grpc.ClientStream
	ctx  context.Context
	once sync.Once
	id   int
}

func (s *fakeWatchStream) Recv() (*kvstorev1.WatchResponse, error) {
	var result *kvstorev1.WatchResponse
	s.once.Do(func() {
		key := fmt.Sprintf("k%d", s.id)
		for i := 0; Shard("mc", key) != s.id && i < 10000; i++ {
			key = fmt.Sprintf("watch-%d-%d", s.id, i)
		}
		result = &kvstorev1.WatchResponse{Namespace: "mc", Key: key, Revision: uint64(20 + s.id)}
	})
	if result != nil {
		return result, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
func (f *fakeShard) Watch(ctx context.Context, _ *kvstorev1.WatchRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[kvstorev1.WatchResponse], error) {
	return &fakeWatchStream{ctx: ctx, id: f.id}, nil
}

type watchSink struct {
	grpc.ServerStream
	ctx  context.Context
	send func(*kvstorev1.WatchResponse) error
}

func (s *watchSink) Context() context.Context { return s.ctx }
func (s *watchSink) Send(r *kvstorev1.WatchResponse) error {
	if s.send == nil {
		return nil
	}
	return s.send(r)
}
func (s *watchSink) SetHeader(metadata.MD) error  { return nil }
func (s *watchSink) SendHeader(metadata.MD) error { return nil }
func (s *watchSink) SetTrailer(metadata.MD)       {}
func TestWatchVectorProgress(t *testing.T) {
	r, _ := newFakeRouter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan *kvstorev1.WatchResponse, 3)
	done := make(chan error, 1)
	go func() {
		done <- r.Watch(&kvstorev1.WatchRequest{Namespace: "mc", UseVectorCursor: true}, &watchSink{ctx: ctx, send: func(v *kvstorev1.WatchResponse) error { received <- v; return nil }})
	}()
	last := Cursor{}
	for i := 0; i < ShardCount; i++ {
		var event *kvstorev1.WatchResponse
		select {
		case event = <-received:
		case <-ctx.Done():
			t.Fatal("context canceled")
		}
		if event.GetCheckpoint() {
			t.Fatal("unexpected checkpoint")
		}
		c, err := DecodeCursor(event.GetCursor())
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < ShardCount; j++ {
			if c[j] < last[j] {
				t.Fatal("cursor went backwards")
			}
		}
		last = c
	}
	if last != (Cursor{20, 21, 22}) {
		t.Fatalf("incomplete vector %v", last)
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected termination: %v", err)
	}
}
