package grpcserver

import (
	"context"
	"encoding/json"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/transaction"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WithTransactions enables the public conditional API. Only Raft voters
// can also accept the private, join-token-authenticated control messages.
func (s *Server) WithTransactions(backend transaction.Backend, allowControl bool) *Server {
	s.transactions, s.allowTransactionControl = backend, allowControl
	return s
}

func (s *Server) Transact(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	if s.transactions == nil {
		return nil, status.Error(codes.Unimplemented, "transactions unavailable on this backend")
	}
	plan, err := transaction.ParseRequest(req)
	if err != nil {
		return nil, err
	}
	result, err := s.transactions.ControlTransaction(ctx, transaction.Control{
		Action: transaction.Local, AtMS: time.Now().UnixMilli(), Plan: &plan,
	})
	if err != nil {
		return nil, mapError(err)
	}
	if result.Outcome != transaction.Committed && result.Outcome != transaction.Aborted {
		return nil, status.Error(codes.Unavailable, "transaction outcome is unknown")
	}
	return &pb.TransactionResponse{Committed: result.Outcome == transaction.Committed}, nil
}

func (s *Server) ControlTransaction(ctx context.Context, req *pb.TransactionControlRequest) (*pb.TransactionControlResponse, error) {
	if !s.allowTransactionControl || s.transactions == nil {
		return nil, status.Error(codes.Unimplemented, "transaction control requires a Raft voter")
	}
	var control transaction.Control
	if len(req.GetPayload()) > transaction.MaxControlPayload || json.Unmarshal(req.GetPayload(), &control) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid transaction control message")
	}
	result, err := s.transactions.ControlTransaction(ctx, control)
	if err != nil {
		return nil, mapError(err)
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.TransactionControlResponse{Payload: payload}, nil
}
