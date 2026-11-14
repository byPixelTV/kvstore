package grpcserver_test

import (
	"context"
	"testing"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	pebblestore "github.com/byPixelTV/kvstore/internal/storage/pebble"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAtomicStandaloneBatchGRPC(t *testing.T) {
	store, err := pebblestore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv := grpcserver.New(kvstore.New(store))
	reply, err := srv.BatchWrite(context.Background(), &kvstorev1.BatchWriteRequest{Operations: []*kvstorev1.BatchWriteOperation{
		{Namespace: "n", Key: "key", Value: []byte("first")},
		{Namespace: "n", Key: "key", Value: []byte("second")},
		{Namespace: "n", Key: "key", Delete: true},
		{Namespace: "n", Key: "key", Delete: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.GetResults()) != 4 {
		t.Fatalf("results: %+v", reply)
	}
	for i, expected := range []uint64{1, 2, 3, 0} {
		if reply.Results[i].GetRevision() != expected {
			t.Fatalf("result %d=%d, expected %d", i, reply.Results[i].GetRevision(), expected)
		}
	}
	if !reply.Results[2].GetDeleted() || reply.Results[3].GetDeleted() {
		t.Fatal("incorrect delete flags")
	}
	if _, err := srv.BatchWrite(context.Background(), &kvstorev1.BatchWriteRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty batch error: %v", err)
	}
}
