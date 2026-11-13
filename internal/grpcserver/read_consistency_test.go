package grpcserver_test

import (
	"context"
	"testing"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	"github.com/byPixelTV/kvstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readModesStore struct {
	strongCalls   int
	eventualCalls int
}

func (s *readModesStore) Get(context.Context, string, string) (storage.Record, bool, error) {
	s.strongCalls++
	return storage.Record{Value: []byte("strong"), Revision: 2}, true, nil
}
func (s *readModesStore) GetLocal(context.Context, string, string) (storage.Record, bool, error) {
	s.eventualCalls++
	return storage.Record{Value: []byte("eventual"), Revision: 1}, true, nil
}
func (*readModesStore) Set(context.Context, string, string, []byte) (uint64, error) { return 0, nil }
func (*readModesStore) Delete(context.Context, string, string) (uint64, error)      { return 0, nil }

func TestGetReadConsistencyOptIn(t *testing.T) {
	store := &readModesStore{}
	server := grpcserver.New(kvstore.New(store))
	ctx := context.Background()
	strong, err := server.Get(ctx, &kvstorev1.GetRequest{Namespace: "n", Key: "k"})
	if err != nil || string(strong.GetValue()) != "strong" || strong.GetRevision() != 2 {
		t.Fatalf("strong=%v err=%v", strong, err)
	}
	eventual, err := server.Get(ctx, &kvstorev1.GetRequest{
		Namespace: "n", Key: "k", Consistency: kvstorev1.ReadConsistency_READ_CONSISTENCY_EVENTUAL,
	})
	if err != nil || string(eventual.GetValue()) != "eventual" || eventual.GetRevision() != 1 {
		t.Fatalf("eventual=%v err=%v", eventual, err)
	}
	if store.strongCalls != 1 || store.eventualCalls != 1 {
		t.Fatalf("wrong read mode dispatch: strong=%d eventual=%d", store.strongCalls, store.eventualCalls)
	}
	_, err = server.Get(ctx, &kvstorev1.GetRequest{Namespace: "n", Key: "k", Consistency: 99})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for unknown mode, got %v", err)
	}
}
