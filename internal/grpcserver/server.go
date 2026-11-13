package grpcserver

import (
	"context"
	"errors"
	"log/slog"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	kvstorev1.UnimplementedKVStoreServer
	service *kvstore.Service
}

var _ kvstorev1.KVStoreServer = (*Server)(nil)

func New(service *kvstore.Service) *Server {
	return &Server{service: service}
}

func (s *Server) Get(
	ctx context.Context,
	req *kvstorev1.GetRequest,
) (*kvstorev1.GetResponse, error) {
	record, found, err := s.service.Get(
		ctx,
		req.GetNamespace(),
		req.GetKey(),
	)
	if err != nil {
		return nil, mapError(err)
	}

	if !found {
		return &kvstorev1.GetResponse{Found: false}, nil
	}

	return &kvstorev1.GetResponse{
		Found:    true,
		Value:    record.Value,
		Revision: record.Revision,
	}, nil
}

func (s *Server) Set(
	ctx context.Context,
	req *kvstorev1.SetRequest,
) (*kvstorev1.SetResponse, error) {
	revision, err := s.service.Set(
		ctx,
		req.GetNamespace(),
		req.GetKey(),
		req.GetValue(),
	)
	if err != nil {
		return nil, mapError(err)
	}

	return &kvstorev1.SetResponse{
		Revision: revision,
	}, nil
}

func (s *Server) Delete(
	ctx context.Context,
	req *kvstorev1.DeleteRequest,
) (*kvstorev1.DeleteResponse, error) {
	revision, err := s.service.Delete(
		ctx,
		req.GetNamespace(),
		req.GetKey(),
	)
	if err != nil {
		return nil, mapError(err)
	}

	return &kvstorev1.DeleteResponse{
		Deleted:  revision != 0,
		Revision: revision,
	}, nil
}

func mapError(err error) error {
	switch {
	case errors.Is(err, kvstore.ErrEmptyNamespace),
		errors.Is(err, kvstore.ErrEmptyKey):
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())

	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())

	default:
		slog.Error("kvstore request failed", "error", err)
		return status.Error(
			codes.Internal,
			"storage operation failed",
		)
	}
}
