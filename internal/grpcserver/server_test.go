package grpcserver_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	"github.com/byPixelTV/kvstore/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func newTestClient(t *testing.T) kvstorev1.KVStoreClient {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	kvstorev1.RegisterKVStoreServer(server, grpcserver.New(kvstore.New(storage.New())))

	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		server.Stop()
		_ = listener.Close()
		t.Fatalf("create gRPC connection: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve gRPC: %v", err)
		}
	})

	return kvstorev1.NewKVStoreClient(conn)
}

func TestGRPCSetGetDelete(t *testing.T) {
	client := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	original := []byte{0, 0xff, 42, 0x00}
	written, err := client.Set(ctx, &kvstorev1.SetRequest{
		Namespace: "players", Key: "123", Value: original,
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if written.GetRevision() != 1 {
		t.Fatalf("Set revision: expected 1, got %d", written.GetRevision())
	}

	got, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: "players", Key: "123"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() || !bytes.Equal(got.GetValue(), original) || got.GetRevision() != 1 {
		t.Fatalf("unexpected Get result: %+v", got)
	}

	// The same key in a different namespace must not exist.
	other, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: "settings", Key: "123"})
	if err != nil || other.GetFound() {
		t.Fatalf("namespace isolation failed: result=%+v error=%v", other, err)
	}

	deleted, err := client.Delete(ctx, &kvstorev1.DeleteRequest{Namespace: "players", Key: "123"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !deleted.GetDeleted() || deleted.GetRevision() != 2 {
		t.Fatalf("unexpected Delete result: %+v", deleted)
	}

	missing, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: "players", Key: "123"})
	if err != nil || missing.GetFound() {
		t.Fatalf("Get after delete: result=%+v error=%v", missing, err)
	}

	deletedAgain, err := client.Delete(ctx, &kvstorev1.DeleteRequest{Namespace: "players", Key: "123"})
	if err != nil || deletedAgain.GetDeleted() || deletedAgain.GetRevision() != 0 {
		t.Fatalf("Delete missing: result=%+v error=%v", deletedAgain, err)
	}
}

func TestGRPCValidation(t *testing.T) {
	client := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, tc := range []struct{ namespace, key string }{
		{namespace: "", key: "123"},
		{namespace: "players", key: ""},
	} {
		_, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: tc.namespace, Key: tc.key})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("Get(%q,%q): expected InvalidArgument, got %v", tc.namespace, tc.key, err)
		}
		_, err = client.Set(ctx, &kvstorev1.SetRequest{Namespace: tc.namespace, Key: tc.key})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("Set(%q,%q): expected InvalidArgument, got %v", tc.namespace, tc.key, err)
		}
		_, err = client.Delete(ctx, &kvstorev1.DeleteRequest{Namespace: tc.namespace, Key: tc.key})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("Delete(%q,%q): expected InvalidArgument, got %v", tc.namespace, tc.key, err)
		}
	}
}
