package sharding

import (
	"context"
	"fmt"
	"sort"
	"sync"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BatchWrite groups operations by their CURRENT shard owner, preserving
// order within each Raft shard. The three groups commit independently; this
// is NOT a transaction across shards. Never auto-retry an uncertain result.
func (r *Router) BatchWrite(ctx context.Context, req *kvstorev1.BatchWriteRequest) (*kvstorev1.BatchWriteResponse, error) {
	items := req.GetOperations()
	if len(items) < 1 || len(items) > 128 {
		return nil, status.Error(codes.InvalidArgument, "BatchWrite requires 1..128 operations")
	}
	size := 0
	keys := make(map[slotKey]struct{}, len(items))
	for _, op := range items {
		if op == nil {
			return nil, status.Error(codes.InvalidArgument, "nil write operation")
		}
		if err := invalidKey(op.GetNamespace(), op.GetKey()); err != nil {
			return nil, err
		}
		size += len(op.GetNamespace()) + len(op.GetKey()) + len(op.GetValue()) + 128
		if size > 256<<10 {
			return nil, status.Error(codes.ResourceExhausted, "batch exceeds 256 KiB; split into smaller batches")
		}
		keys[slotKey{op.GetNamespace(), Slot(op.GetNamespace(), op.GetKey())}] = struct{}{}
	}
	if err := r.checkRouteAvailable(); err != nil {
		return nil, err
	}

	// Acquire EVERY involved slot read-lock in stable order. This prevents a
	// migration cutover between the routing decision and the shard commit.
	// The router never holds unrelated slots locked during a batch.
	slots := make([]slotKey, 0, len(keys))
	for slot := range keys {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].namespace != slots[j].namespace {
			return slots[i].namespace < slots[j].namespace
		}
		return slots[i].slot < slots[j].slot
	})
	gates := make([]*sync.RWMutex, 0, len(slots))
	for _, slot := range slots {
		gate := r.slotGate(slot.namespace, slot.slot)
		gate.RLock()
		gates = append(gates, gate)
	}
	defer func() {
		for i := len(gates) - 1; i >= 0; i-- {
			gates[i].RUnlock()
		}
	}()
	if err := r.checkRouteAvailable(); err != nil {
		return nil, err
	}

	layout := r.current()
	type shardPart struct {
		positions  []int
		operations []*kvstorev1.BatchWriteOperation
	}
	var parts [ShardCount]shardPart
	for i, op := range items {
		shard := layout.owner(op.GetNamespace(), Slot(op.GetNamespace(), op.GetKey()))
		parts[shard].positions = append(parts[shard].positions, i)
		parts[shard].operations = append(parts[shard].operations, op)
	}

	results := make([]*kvstorev1.BatchWriteResult, len(items))
	var wg sync.WaitGroup
	for shard, part := range parts {
		if len(part.positions) == 0 {
			continue
		}
		wg.Add(1)
		go func(shard int, part shardPart) {
			defer wg.Done()
			completeError := func(err error) {
				code := status.Code(err)
				// Errors from in-flight Raft RPCs may represent UNKNOWN OUTCOME.
				// Return an item status rather than reporting partial success.
				for _, pos := range part.positions {
					results[pos] = &kvstorev1.BatchWriteResult{GrpcCode: uint32(code), Error: status.Convert(err).Message()}
				}
			}
			client, err := r.groups[shard].selectClient()
			if err != nil {
				completeError(err)
				return
			}
			reply, err := client.BatchWrite(ctx, &kvstorev1.BatchWriteRequest{Operations: part.operations})
			if err != nil {
				if status.Code(err) == codes.Unavailable {
					r.groups[shard].leader.Store(-1)
				}
				completeError(err)
				return
			}
			if len(reply.GetResults()) != len(part.positions) {
				completeError(status.Error(codes.Internal, fmt.Sprintf("shard %d returned unexpected result count", shard)))
				return
			}
			for i, item := range reply.GetResults() {
				if item == nil {
					results[part.positions[i]] = &kvstorev1.BatchWriteResult{GrpcCode: uint32(codes.Internal), Error: "shard returned missing result"}
					continue
				}
				copyItem := *item
				if item.GetGrpcCode() == 0 && item.GetRevision() != 0 {
					encoded, e := EncodeRevision(shard, item.GetRevision())
					if e != nil {
						results[part.positions[i]] = &kvstorev1.BatchWriteResult{GrpcCode: uint32(codes.Internal), Error: e.Error()}
						continue
					}
					copyItem.Revision = encoded
				}
				results[part.positions[i]] = &copyItem
			}
		}(shard, part)
	}
	wg.Wait()
	return &kvstorev1.BatchWriteResponse{Results: results}, nil
}
