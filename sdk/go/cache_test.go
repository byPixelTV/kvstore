package kvstore_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	sdk "github.com/byPixelTV/kvstore/sdk/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestNamespaceCacheMaterializesChangesAndExpiry(t *testing.T) {
	client := testClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	namespace := fmt.Sprintf("sdk-go-cache-%d", time.Now().UnixNano())
	if _, err := client.Set(ctx, namespace, "initial", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	cache, err := client.CacheNamespace(ctx, namespace)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	snapshot, version := cache.Snapshot()
	record, found := cache.Get("initial")
	if !found || string(record.Value) != "value" || string(snapshot["initial"].Value) != "value" {
		t.Fatalf("incomplete cache snapshot: %+v", snapshot)
	}
	record.Value[0] = 'X'
	snapshot["initial"].Value[0] = 'Y'
	if record, _ := cache.Get("initial"); string(record.Value) != "value" {
		t.Fatalf("cache shared mutable bytes: %q", record.Value)
	}

	expires := time.Now().Add(time.Second).UnixMilli()
	written, err := client.Set(ctx, namespace, "expiring", []byte("live"), expires)
	if err != nil {
		t.Fatal(err)
	}
	version, err = cache.WaitForChange(ctx, version)
	if err != nil {
		t.Fatal(err)
	}
	if record, found := cache.Get("expiring"); !found || record.Revision != written.GetRevision() || record.ExpiresAtUnixMS != expires {
		t.Fatalf("cache did not observe a live update: %+v, found=%v", record, found)
	}
	version, err = cache.WaitForChange(ctx, version)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := cache.Get("expiring"); found {
		t.Fatal("cache retained an expired record")
	}
	if _, err := client.Delete(ctx, namespace, "initial"); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.WaitForChange(ctx, version); err != nil {
		t.Fatal(err)
	}
	if snapshot, _ := cache.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("cache retained deleted records: %+v", snapshot)
	}
}

func TestNamespaceCacheResumesAndReplacesSnapshotAfterCursorLoss(t *testing.T) {
	server := &cacheScriptServer{
		resumed: make(chan struct{}), rescanning: make(chan struct{}), replace: make(chan struct{}),
	}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterKVStoreServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///cache", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := sdk.FromConn(conn, "")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cache, err := client.CacheNamespace(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	select {
	case <-server.rescanning:
	case <-cache.Done():
		t.Fatalf("cache stopped before rescanning: %v", cache.Err())
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if snapshot, _ := cache.Snapshot(); !cache.Stale() || len(snapshot) != 2 || string(snapshot["updated"].Value) != "old update" {
		t.Fatalf("rescan did not preserve and mark the previous snapshot: %+v, stale=%v", snapshot, cache.Stale())
	}
	close(server.replace)
	select {
	case <-server.resumed:
	case <-cache.Done():
		t.Fatalf("cache stopped before resuming: %v", cache.Err())
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	snapshot, version := cache.Snapshot()
	if cache.Stale() || len(snapshot) != 1 || string(snapshot["replacement"].Value) != "new" || version < 4 {
		t.Fatalf("rescan did not replace the cache atomically: %+v, version=%d", snapshot, version)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.WaitForChange(ctx, version); !errors.Is(err, context.Canceled) {
		t.Fatalf("closing a Client did not stop its cache: %v", err)
	}
}

type cacheScriptServer struct {
	pb.UnimplementedKVStoreServer
	scans, watches atomic.Int32
	resumed        chan struct{}
	rescanning     chan struct{}
	replace        chan struct{}
}

func (s *cacheScriptServer) Scan(_ *pb.ScanRequest, stream grpc.ServerStreamingServer[pb.ScanResponse]) error {
	switch s.scans.Add(1) {
	case 1:
		if err := stream.Send(&pb.ScanResponse{Key: "initial", Value: []byte("old")}); err != nil {
			return err
		}
		return stream.Send(&pb.ScanResponse{Checkpoint: true, Cursor: "initial"})
	case 2:
		if err := stream.Send(&pb.ScanResponse{Key: "partial", Value: []byte("must not leak")}); err != nil {
			return err
		}
		return status.Error(codes.Aborted, "routing changed during scan")
	case 3:
		close(s.rescanning)
		select {
		case <-s.replace:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		if err := stream.Send(&pb.ScanResponse{Key: "replacement", Value: []byte("new")}); err != nil {
			return err
		}
		return stream.Send(&pb.ScanResponse{Checkpoint: true, Cursor: "replacement"})
	default:
		return status.Error(codes.Internal, "unexpected scan")
	}
}

func (s *cacheScriptServer) Watch(req *pb.WatchRequest, stream grpc.ServerStreamingServer[pb.WatchResponse]) error {
	switch s.watches.Add(1) {
	case 1:
		if req.GetCursor() != "initial" {
			return status.Error(codes.InvalidArgument, "initial watch did not use the scan checkpoint")
		}
		if err := stream.Send(&pb.WatchResponse{Namespace: "n", Key: "updated", Value: []byte("old update"), Cursor: "data"}); err != nil {
			return err
		}
		if err := stream.Send(&pb.WatchResponse{Checkpoint: true, Cursor: "checkpoint"}); err != nil {
			return err
		}
		return nil // Clean stream termination must also reconnect.
	case 2:
		if req.GetCursor() != "checkpoint" {
			return status.Error(codes.InvalidArgument, "reconnect did not use the latest watch checkpoint")
		}
		return status.Error(codes.FailedPrecondition, "stale cursor")
	case 3:
		if req.GetCursor() != "replacement" {
			return status.Error(codes.InvalidArgument, "watch did not resume after the replacement snapshot")
		}
		close(s.resumed)
		<-stream.Context().Done()
		return stream.Context().Err()
	default:
		return status.Error(codes.Internal, "unexpected watch")
	}
}
