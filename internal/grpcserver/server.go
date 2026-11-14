package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/hashicorp/raft"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	"github.com/byPixelTV/kvstore/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	kvstorev1.UnimplementedKVStoreServer
	service *kvstore.Service
	fence   interface {
		FenceSlot(context.Context, string, uint16, bool) error
	}
}

var _ kvstorev1.KVStoreServer = (*Server)(nil)

func New(service *kvstore.Service, fencer ...interface {
	FenceSlot(context.Context, string, uint16, bool) error
}) *Server {
	s := &Server{service: service}
	if len(fencer) > 0 {
		s.fence = fencer[0]
	}
	return s
}

func (s *Server) FenceSlot(ctx context.Context, req *kvstorev1.FenceSlotRequest) (*kvstorev1.FenceSlotResponse, error) {
	if s.fence == nil {
		return nil, status.Error(codes.Unimplemented, "slot fences require a Raft backend")
	}
	if req.GetNamespace() == "" || req.GetSlot() >= 4096 {
		return nil, status.Error(codes.InvalidArgument, "invalid slot fence")
	}
	if err := s.fence.FenceSlot(ctx, req.GetNamespace(), uint16(req.GetSlot()), req.GetSealed()); err != nil {
		return nil, mapError(err)
	}
	return &kvstorev1.FenceSlotResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *kvstorev1.GetRequest) (*kvstorev1.GetResponse, error) {
	var record storage.Record
	var found bool
	var err error
	switch req.GetConsistency() {
	case kvstorev1.ReadConsistency_READ_CONSISTENCY_STRONG:
		record, found, err = s.service.Get(ctx, req.GetNamespace(), req.GetKey())
	case kvstorev1.ReadConsistency_READ_CONSISTENCY_EVENTUAL:
		record, found, err = s.service.GetEventual(ctx, req.GetNamespace(), req.GetKey())
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown read consistency")
	}
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

func (s *Server) Set(ctx context.Context, req *kvstorev1.SetRequest) (*kvstorev1.SetResponse, error) {
	revision, err := s.service.Set(ctx, req.GetNamespace(), req.GetKey(), req.GetValue())
	if err != nil {
		return nil, mapError(err)
	}
	return &kvstorev1.SetResponse{Revision: revision}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvstorev1.DeleteRequest) (*kvstorev1.DeleteResponse, error) {
	revision, err := s.service.Delete(ctx, req.GetNamespace(), req.GetKey())
	if err != nil {
		return nil, mapError(err)
	}
	return &kvstorev1.DeleteResponse{
		Deleted:  revision != 0,
		Revision: revision,
	}, nil
}

func mapError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, kvstore.ErrEmptyNamespace), errors.Is(err, kvstore.ErrEmptyKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost):
		return status.Error(codes.Unavailable, "Raft leader unavailable")
	case errors.Is(err, changefeed.ErrHistoryUnavailable):
		return status.Error(codes.OutOfRange, "changefeed history unavailable; rebuild cache from authoritative values")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		slog.Error("kvstore request failed", "error", err)
		return status.Error(codes.Internal, "storage operation failed")
	}
}

func (s *Server) Head(ctx context.Context, _ *kvstorev1.HeadRequest) (*kvstorev1.HeadResponse, error) {
	feed, ok := s.service.Feed()
	if !ok {
		return nil, status.Error(codes.Unimplemented, "changefeed unsupported by backend")
	}
	head, oldest, err := feed.Head(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return &kvstorev1.HeadResponse{Revision: head, OldestAvailableRevision: oldest}, nil
}

// Watch replays changes from durable storage before following new commits.
// Capturing Notify() before each read prevents a lost wakeup during catchup.
func (s *Server) Watch(req *kvstorev1.WatchRequest, stream grpc.ServerStreamingServer[kvstorev1.WatchResponse]) error {
	if req.GetNamespace() == "" {
		return status.Error(codes.InvalidArgument, "namespace cannot be empty")
	}
	feed, ok := s.service.Feed()
	if !ok {
		return status.Error(codes.Unimplemented, "changefeed unsupported by backend")
	}
	cursor := req.GetAfterRevision()
	ticker := time.NewTicker(20 * time.Second) // keep Envoy's streaming idle timeout from closing quiet watches
	defer ticker.Stop()
	var checkpointed bool
	for {
		signal := feed.Notify()
		page, err := feed.ReadChanges(stream.Context(), cursor, 512)
		if err != nil {
			return mapError(err)
		}
		lastSent := uint64(0)
		for _, event := range page.Events {
			cursor = event.Revision
			if event.Namespace != req.GetNamespace() || (req.GetKey() != "" && event.Key != req.GetKey()) {
				continue
			}
			if err := stream.Send(&kvstorev1.WatchResponse{Revision: event.Revision, Namespace: event.Namespace, Key: event.Key, Value: event.Value, Deleted: event.Deleted}); err != nil {
				return err
			}
			lastSent = cursor
		}
		if len(page.Events) > 0 && cursor < page.Head {
			continue
		} // bounded catch-up pages
		if cursor < page.Head {
			cursor = page.Head
		}
		if !checkpointed || (cursor != lastSent && (lastSent != 0 || len(page.Events) > 0)) {
			if err := stream.Send(&kvstorev1.WatchResponse{Revision: cursor, Checkpoint: true}); err != nil {
				return err
			}
			checkpointed = true
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-signal:
		case <-ticker.C:
			// An idle heartbeat is also a resumable cursor checkpoint.
			if err := stream.Send(&kvstorev1.WatchResponse{Revision: cursor, Checkpoint: true}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) Scan(req *kvstorev1.ScanRequest, stream grpc.ServerStreamingServer[kvstorev1.ScanResponse]) error {
	err := s.service.ScanNamespace(stream.Context(), req.GetNamespace(), func(item storage.ScanItem) error {
		return stream.Send(&kvstorev1.ScanResponse{SnapshotRevision: item.SnapshotRevision, Key: item.Key, Value: item.Value, Revision: item.Revision, Checkpoint: item.Checkpoint})
	})
	if err != nil {
		return mapError(err)
	}
	return nil
}
