package grpcserver

import (
	"context"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) BatchWrite(ctx context.Context, req *kvstorev1.BatchWriteRequest) (*kvstorev1.BatchWriteResponse, error) {
	if len(req.GetOperations()) == 0 || len(req.GetOperations()) > 128 {
		return nil, status.Error(codes.InvalidArgument, "BatchWrite requires 1..128 operations")
	}
	total := 0
	operations := make([]storage.WriteOperation, len(req.GetOperations()))
	for i, op := range req.GetOperations() {
		if op == nil || op.GetNamespace() == "" || op.GetKey() == "" {
			return nil, status.Error(codes.InvalidArgument, "BatchWrite contains empty namespace/key")
		}
		if len(op.GetValue()) > 256<<10 {
			return nil, status.Error(codes.ResourceExhausted, "value exceeds 256 KiB")
		}
		total += len(op.GetNamespace()) + len(op.GetKey()) + len(op.GetValue()) + 128
		if total > 256<<10 {
			return nil, status.Error(codes.ResourceExhausted, "batch exceeds 256 KiB; split into smaller batches")
		}
		operations[i] = storage.WriteOperation{Namespace: op.GetNamespace(), Key: op.GetKey(), Value: op.GetValue(), Delete: op.GetDelete(), ExpiresAtUnixMS: op.GetExpiresAtUnixMs()}
	}
	revisions, err := s.service.BatchWrite(ctx, operations)
	if err != nil {
		return nil, mapError(err)
	}
	if len(revisions) != len(operations) {
		return nil, status.Error(codes.Internal, "wrong batch result count")
	}
	result := &kvstorev1.BatchWriteResponse{Results: make([]*kvstorev1.BatchWriteResult, len(revisions))}
	for i, rev := range revisions {
		result.Results[i] = &kvstorev1.BatchWriteResult{Revision: rev, Deleted: operations[i].Delete && rev != 0}
	}
	return result, nil
}
