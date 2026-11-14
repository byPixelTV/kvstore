package cluster

import (
	"context"
	"encoding/json"
	"errors"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ storage.BatchWriter = (*Node)(nil)

// BatchWrite replicates exactly one ordered opBatch command per RPC. The
// existing FSM applies it as ONE synchronous Pebble commit. A failed RPC
// may already have committed; clients must reconcile rather than retrying.
func (n *Node) BatchWrite(ctx context.Context, operations []storage.WriteOperation) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(operations) < 1 || len(operations) > 128 {
		return nil, status.Error(codes.InvalidArgument, "invalid batch length")
	}
	commands := make([]command, 0, len(operations))
	total := 0
	for _, op := range operations {
		if op.Namespace == "" || op.Key == "" {
			return nil, status.Error(codes.InvalidArgument, "invalid namespace/key")
		}
		if len(op.Value) > 256<<10 {
			return nil, status.Error(codes.ResourceExhausted, "value exceeds 256 KiB")
		}
		cmd := command{Operation: opSet, Namespace: op.Namespace, Key: op.Key}
		if op.Delete {
			cmd.Operation = opDelete
		} else {
			cmd.Value = op.Value
		}
		total += estimateRaftCommand(cmd)
		if total > clusterWriteBatchBytes {
			return nil, status.Error(codes.ResourceExhausted, "batch exceeds Raft log budget")
		}
		commands = append(commands, cmd)
	}
	if n.raft.State() != raft.Leader {
		peer, err := n.leaderPeer()
		if err != nil {
			return nil, err
		}
		if peer.ID == n.cfg.NodeID {
			return nil, status.Error(codes.Unavailable, "leader transition")
		}
		client, err := n.client(peer.GRPCAddr)
		if err != nil {
			return nil, err
		}
		req := &kvstorev1.BatchWriteRequest{Operations: make([]*kvstorev1.BatchWriteOperation, len(operations))}
		for i, op := range operations {
			req.Operations[i] = &kvstorev1.BatchWriteOperation{Namespace: op.Namespace, Key: op.Key, Value: op.Value, Delete: op.Delete}
		}
		res, err := client.BatchWrite(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(res.GetResults()) != len(operations) {
			return nil, errors.New("forwarded batch returned incorrect result count")
		}
		revisions := make([]uint64, len(operations))
		for i, item := range res.Results {
			if item == nil {
				return nil, errors.New("missing forwarded batch item")
			}
			if item.GetGrpcCode() != 0 {
				return nil, status.Error(codes.Code(item.GetGrpcCode()), item.GetError())
			}
			revisions[i] = item.GetRevision()
		}
		return revisions, nil
	}
	cmd := command{Operation: opBatch, Commands: commands}
	encoded, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	if len(encoded) > clusterWriteBatchBytes {
		return nil, status.Error(codes.ResourceExhausted, "encoded batch exceeds Raft log budget")
	}
	result, err := n.applyDirect(ctx, cmd)
	if err != nil {
		return nil, err
	}
	if len(result.Revisions) != len(commands) {
		return nil, errors.New("Raft batch result count mismatch")
	}
	return result.Revisions, nil
}
