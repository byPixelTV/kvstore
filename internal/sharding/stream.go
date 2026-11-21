package sharding

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Scan exposes a namespace snapshot per shard and emits a FINAL checkpoint
// carrying all three snapshot cursors. It is NOT one globally atomic snapshot.
// Start Watch(cursor=finalCheckpoint.cursor) to catch changes written while
// scanning earlier shards; this avoids a resync gap without distributed locks.
func (r *Router) Scan(req *kvstorev1.ScanRequest, stream grpc.ServerStreamingServer[kvstorev1.ScanResponse]) error {
	if req.GetNamespace() == "" {
		return status.Error(codes.InvalidArgument, "namespace cannot be empty")
	}
	if err := invalidKey(req.GetNamespace(), "_scan_watch_"); err != nil {
		return err
	}
	if err := r.checkRouteAvailable(); err != nil {
		return err
	}
	if err := requireV2(req.GetUseVectorCursor()); err != nil {
		return err
	}
	epoch, _ := r.epochSignal()
	var cursors Cursor
	for id, g := range r.groups {
		client, err := g.selectClient()
		if err != nil {
			return err
		}
		in, err := client.Scan(stream.Context(), &kvstorev1.ScanRequest{Namespace: req.GetNamespace()})
		if err != nil {
			return err
		}
		sawCheckpoint := false
		for {
			res, err := in.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("shard %d scan: %w", id, err)
			}
			if res.GetCheckpoint() {
				if sawCheckpoint {
					return status.Error(codes.Internal, "duplicate shard scan checkpoint")
				}
				sawCheckpoint = true
				cursors[id] = res.GetSnapshotRevision()
				continue
			}
			if !sawCheckpoint {
				return status.Error(codes.Internal, "missing initial shard scan checkpoint")
			}
			if r.current().Epoch != epoch {
				return status.Error(codes.Aborted, "routing changed during Scan; discard partial result and retry")
			}
			if r.current().owner(req.GetNamespace(), Slot(req.GetNamespace(), res.GetKey())) != id {
				continue
			}
			rev, e := EncodeRevision(id, res.GetRevision())
			if e != nil {
				return status.Error(codes.Internal, e.Error())
			}
			if err := stream.Send(&kvstorev1.ScanResponse{Key: res.GetKey(), Value: res.GetValue(), Revision: rev, Shard: uint32(id), ExpiresAtUnixMs: res.GetExpiresAtUnixMs()}); err != nil {
				return err
			}
		}
		if !sawCheckpoint {
			return status.Error(codes.Internal, "empty shard scan without checkpoint")
		}
	}
	if r.current().Epoch != epoch {
		return status.Error(codes.Aborted, "routing changed during Scan; retry")
	}
	return stream.Send(&kvstorev1.ScanResponse{Checkpoint: true, Cursor: encodeEpochCursor(cursors, epoch)})
}

type shardWatchEvent struct {
	shard int
	event *kvstorev1.WatchResponse
	err   error
}

// Watch fans out to ALL shard leaders and merges events. Ordering is exact
// within each shard; events across shards have no defined total order.
// Failed shard streams fail the entire stream rather than silently dropping
// invalidations. Clients reconnect using the LAST received opaque cursor.
func (r *Router) Watch(req *kvstorev1.WatchRequest, stream grpc.ServerStreamingServer[kvstorev1.WatchResponse]) error {
	if req.GetNamespace() == "" {
		return status.Error(codes.InvalidArgument, "namespace cannot be empty")
	}
	if err := invalidKey(req.GetNamespace(), "_scan_watch_"); err != nil {
		return err
	}
	if err := r.checkRouteAvailable(); err != nil {
		return err
	}
	if err := requireV2(req.GetUseVectorCursor()); err != nil {
		return err
	}
	if req.GetAfterRevision() != 0 {
		return status.Error(codes.InvalidArgument, "use cursor, not after_revision, for sharded watch")
	}
	epoch, changed := r.epochSignal()
	cursors, err := decodeEpochCursor(req.GetCursor(), epoch)
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	events := make(chan shardWatchEvent, 128)
	var wg sync.WaitGroup
	for id, g := range r.groups {
		client, e := g.selectClient()
		if e != nil {
			return e
		}
		wg.Add(1)
		go func(id int, client kvstorev1.KVStoreClient, starting uint64) {
			defer wg.Done()
			feed, e := client.Watch(ctx, &kvstorev1.WatchRequest{Namespace: req.GetNamespace(), Key: req.GetKey(), AfterRevision: starting})
			if e != nil {
				select {
				case events <- shardWatchEvent{shard: id, err: e}:
				case <-ctx.Done():
				}
				return
			}
			for {
				change, e := feed.Recv()
				if e != nil {
					if ctx.Err() == nil {
						if errors.Is(e, io.EOF) {
							e = status.Error(codes.Unavailable, "shard watch ended")
						}
						select {
						case events <- shardWatchEvent{shard: id, err: e}:
						case <-ctx.Done():
						}
					}
					return
				}
				select {
				case events <- shardWatchEvent{shard: id, event: change}:
				case <-ctx.Done():
					return
				}
			}
		}(id, client, cursors[id])
	}
	go func() { wg.Wait(); close(events) }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			return status.Error(codes.FailedPrecondition, "shard ownership changed; rescan namespace before resuming watch")
		case e, ok := <-events:
			if !ok {
				return status.Error(codes.Unavailable, "all shard feeds ended")
			}
			if e.err != nil {
				return fmt.Errorf("shard %d watch: %w", e.shard, e.err)
			}
			if r.current().Epoch != epoch {
				return status.Error(codes.FailedPrecondition, "shard ownership changed; rescan required")
			}
			incoming := e.event.GetRevision()
			if incoming < cursors[e.shard] {
				return status.Error(codes.Internal, "watch revision went backwards")
			}
			// The new cursor is emitted only after the corresponding change is
			// delivered successfully. An uncertain send may repeat an event after
			// reconnect; clients should apply events idempotently by shard/revision.
			next := cursors
			next[e.shard] = incoming
			owned := e.event.GetCheckpoint() || r.current().owner(req.GetNamespace(), Slot(req.GetNamespace(), e.event.GetKey())) == e.shard
			response := &kvstorev1.WatchResponse{
				Revision:  0, // no single global revision; per-shard revision below
				Namespace: e.event.GetNamespace(), Key: e.event.GetKey(), Value: e.event.GetValue(),
				Deleted: e.event.GetDeleted(), Checkpoint: e.event.GetCheckpoint(),
				ExpiresAtUnixMs: e.event.GetExpiresAtUnixMs(),
				Cursor:          encodeEpochCursor(next, epoch), Shard: uint32(e.shard),
			}
			if !owned {
				response.Checkpoint = true
				response.Key = ""
				response.Value = nil
				response.Deleted = false
				response.ExpiresAtUnixMs = 0
				response.Namespace = ""
			}
			if !response.Checkpoint {
				rev, err := EncodeRevision(e.shard, incoming)
				if err != nil {
					return status.Error(codes.Internal, err.Error())
				}
				response.Revision = rev
			}
			if err := stream.Send(response); err != nil {
				return err
			}
			cursors = next
		}
	}
}
