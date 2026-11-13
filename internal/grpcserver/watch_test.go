package grpcserver_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	pebblestore "github.com/byPixelTV/kvstore/internal/storage/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCWatchResumeAndScan(t *testing.T) {
	store, err := pebblestore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	kvstorev1.RegisterKVStoreServer(server, grpcserver.New(kvstore.New(store)))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := kvstorev1.NewKVStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	head, err := client.Head(ctx, &kvstorev1.HeadRequest{})
	if err != nil || head.GetRevision() != 0 {
		t.Fatalf("head=%+v err=%v", head, err)
	}
	watch, err := client.Watch(ctx, &kvstorev1.WatchRequest{Namespace: "n", AfterRevision: 0})
	if err != nil {
		t.Fatal(err)
	}
	if initial, err := watch.Recv(); err != nil || !initial.GetCheckpoint() {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	_, err = client.Set(ctx, &kvstorev1.SetRequest{Namespace: "n", Key: "k", Value: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := watch.Recv()
	if err != nil || msg.GetRevision() != 1 || msg.GetKey() != "k" || string(msg.GetValue()) != "first" {
		t.Fatalf("change=%+v err=%v", msg, err)
	}
	scan, err := client.Scan(ctx, &kvstorev1.ScanRequest{Namespace: "n"})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := scan.Recv()
	if err != nil || !initial.GetCheckpoint() || initial.GetSnapshotRevision() != 1 {
		t.Fatalf("snapshot start=%+v err=%v", initial, err)
	}
	item, err := scan.Recv()
	if err != nil || item.GetKey() != "k" || item.GetRevision() != 1 {
		t.Fatalf("entry=%+v err=%v", item, err)
	}
	_, err = scan.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	_, err = client.Delete(ctx, &kvstorev1.DeleteRequest{Namespace: "n", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := client.Watch(ctx, &kvstorev1.WatchRequest{Namespace: "n", AfterRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	deletion, err := replay.Recv()
	if err != nil || deletion.GetRevision() != 2 || !deletion.GetDeleted() {
		t.Fatalf("delete event=%+v err=%v", deletion, err)
	}
}
