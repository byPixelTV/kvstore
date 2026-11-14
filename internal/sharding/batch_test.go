package sharding

import (
	"context"
	"fmt"
	"testing"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BatchWrite fake preserves each shard-local item order while returning
// independently successful/failed shard outcomes.
func (f *fakeShard) BatchWrite(_ context.Context, req *kvstorev1.BatchWriteRequest, _ ...grpc.CallOption) (*kvstorev1.BatchWriteResponse, error) {
	f.batchAttempts++
	if f.failSet {
		return nil, status.Error(codes.Unavailable, "outcome uncertain")
	}
	f.writes += len(req.GetOperations())
	res := &kvstorev1.BatchWriteResponse{Results: make([]*kvstorev1.BatchWriteResult, len(req.GetOperations()))}
	for i, op := range req.GetOperations() {
		res.Results[i] = &kvstorev1.BatchWriteResult{Revision: uint64(10 + i), Deleted: op.GetDelete()}
	}
	return res, nil
}

func TestRouterBatchRoutesByShardPreservingResponseOrder(t *testing.T) {
	router, shards := newFakeRouter()
	var key [ShardCount]string
	for i := 0; i < ShardCount; i++ {
		for j := 0; j < 10000; j++ {
			candidate := fmt.Sprintf("batch-%d", j)
			if Shard("players", candidate) == i {
				key[i] = candidate
				break
			}
		}
		if key[i] == "" {
			t.Fatalf("missing key for shard %d", i)
		}
	}
	reply, err := router.BatchWrite(context.Background(), &kvstorev1.BatchWriteRequest{Operations: []*kvstorev1.BatchWriteOperation{
		{Namespace: "players", Key: key[2], Value: []byte("a")},
		{Namespace: "players", Key: key[0], Value: []byte("b")},
		{Namespace: "players", Key: key[2], Delete: true},
		{Namespace: "players", Key: key[1], Value: []byte("c")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.GetResults()) != 4 {
		t.Fatalf("results: %+v", reply)
	}
	wantShards := []int{2, 0, 2, 1}
	wantRevs := []uint64{10, 10, 11, 10}
	for i, item := range reply.Results {
		expected, err := EncodeRevision(wantShards[i], wantRevs[i])
		if err != nil {
			t.Fatal(err)
		}
		if item.GetGrpcCode() != 0 || item.GetRevision() != expected {
			t.Fatalf("result %d %+v want rev %d", i, item, expected)
		}
	}
	for i, expected := range []int{1, 1, 2} {
		if shards[i].writes != expected {
			t.Fatalf("shard %d writes=%d want %d", i, shards[i].writes, expected)
		}
	}
}

func TestRouterBatchPartialFailureNeverRetries(t *testing.T) {
	router, shards := newFakeRouter()
	shards[1].failSet = true
	var opKeys [2]string
	for j := 0; j < 1000; j++ {
		k := fmt.Sprintf("key-%d", j)
		if Shard("ns", k) == 0 {
			opKeys[0] = k
		}
		if Shard("ns", k) == 1 {
			opKeys[1] = k
		}
		if opKeys[0] != "" && opKeys[1] != "" {
			break
		}
	}
	res, err := router.BatchWrite(context.Background(), &kvstorev1.BatchWriteRequest{Operations: []*kvstorev1.BatchWriteOperation{
		{Namespace: "ns", Key: opKeys[0], Value: []byte("ok")},
		{Namespace: "ns", Key: opKeys[1], Value: []byte("uncertain")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GetResults()) != 2 || res.Results[0].GetGrpcCode() != 0 || codes.Code(res.Results[1].GetGrpcCode()) != codes.Unavailable {
		t.Fatalf("partial outcome: %+v", res)
	}
	if shards[0].batchAttempts != 1 || shards[1].batchAttempts != 1 {
		t.Fatalf("uncertain write retried: attempts=%d", shards[1].batchAttempts)
	}
	if shards[0].writes != 1 || shards[1].writes != 0 {
		t.Fatalf("unexpected attempts shard0=%d shard1=%d", shards[0].writes, shards[1].writes)
	}
}
