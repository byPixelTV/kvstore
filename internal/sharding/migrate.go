package sharding

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MigrationReport summarizes one namespace/slot transfer. Source and target
// each use their own Raft log, and routing metadata is committed to shard 0.
type MigrationReport struct {
	Namespace       string  `json:"namespace"`
	Slot            uint16  `json:"slot"`
	Source          int     `json:"source"`
	Target          int     `json:"target"`
	OldEpoch        uint64  `json:"old_epoch"`
	NewEpoch        uint64  `json:"new_epoch"`
	Copied          uint64  `json:"copied"`
	Replayed        uint64  `json:"replayed"`
	ReplayWrites    uint64  `json:"replay_writes"`
	RemovedStale    uint64  `json:"removed_stale"`
	CutoverFreezeMS float64 `json:"cutover_freeze_ms"`
}

func (r *Router) backend(shard int) (kvstorev1.KVStoreClient, error) {
	if shard < 0 || shard >= ShardCount {
		return nil, status.Error(codes.InvalidArgument, "invalid destination shard")
	}
	return r.groups[shard].selectClient()
}

func scanSlot(ctx context.Context, client kvstorev1.KVStoreClient, namespace string, slot uint16, fn func(*kvstorev1.ScanResponse) error) (uint64, error) {
	stream, err := client.Scan(ctx, &kvstorev1.ScanRequest{Namespace: namespace})
	if err != nil {
		return 0, err
	}
	var seen bool
	var revision uint64
	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if !seen {
				return 0, status.Error(codes.Internal, "scan missing snapshot checkpoint")
			}
			return revision, nil
		}
		if err != nil {
			return 0, err
		}
		if res.GetCheckpoint() {
			if seen {
				return 0, status.Error(codes.Internal, "duplicate snapshot checkpoint")
			}
			seen, revision = true, res.GetSnapshotRevision()
			continue
		}
		if !seen {
			return 0, status.Error(codes.Internal, "snapshot record before checkpoint")
		}
		if Slot(namespace, res.GetKey()) != slot {
			continue
		}
		if err := fn(res); err != nil {
			return 0, err
		}
	}
}

// parallelSlotScan copies independent keys with bounded concurrency. Each
// worker waits for its Raft-acknowledged mutation before fetching another
// record. The later changefeed replay remains serial to preserve per-key order.
func parallelSlotScan(ctx context.Context, client kvstorev1.KVStoreClient, namespace string, slot uint16, workers int, fn func(context.Context, *kvstorev1.ScanResponse) error) (uint64, uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan *kvstorev1.ScanResponse, workers*2)
	var completed atomic.Uint64
	var firstErr error
	var once sync.Once
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := fn(ctx, item); err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
				completed.Add(1)
			}
		}()
	}
	revision, scanErr := scanSlot(ctx, client, namespace, slot, func(item *kvstorev1.ScanResponse) error {
		copied := &kvstorev1.ScanResponse{Key: item.GetKey(), Value: append([]byte(nil), item.GetValue()...), Revision: item.GetRevision(), ExpiresAtUnixMs: item.GetExpiresAtUnixMs()}
		select {
		case jobs <- copied:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return 0, completed.Load(), firstErr
	}
	if scanErr != nil {
		return 0, completed.Load(), scanErr
	}
	return revision, completed.Load(), nil
}

// Replay batches hold only the latest mutation for each key. All changes
// remain ordered by source revision, but *intermediate* values of a hot key
// need not be persisted on the destination. No target reads can reach that
// key until the routing cutover succeeds and the source has been sealed.
// The upper bounds limit memory use for namespaces with many distinct keys.
const (
	migrationReplayMaxKeys   = 128
	migrationReplayMaxBytes  = 4 << 20
	migrationReplayMaxEvents = 8192
	migrationReplayWorkers   = 16
)

type replayMutation struct {
	key             string
	value           []byte
	deleted         bool
	expiresAtUnixMS int64
}

// applyReplay writes distinct keys in parallel but does not return until ALL
// acknowledged destination writes have completed. Applying the next batch
// only after this one preserves per-key order across replay batches.
func applyReplay(ctx context.Context, target kvstorev1.KVStoreClient, namespace string, pending map[string]replayMutation) error {
	if len(pending) == 0 {
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan replayMutation)
	workers := min(len(pending), migrationReplayWorkers)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if ctx.Err() != nil {
					return
				}
				var err error
				if item.deleted {
					_, err = target.Delete(ctx, &kvstorev1.DeleteRequest{Namespace: namespace, Key: item.key})
				} else {
					_, err = setOnBackend(ctx, target, &kvstorev1.SetRequest{Namespace: namespace, Key: item.key, Value: item.value, ExpiresAtUnixMs: item.expiresAtUnixMS})
				}
				if err != nil {
					once.Do(func() { firstErr = fmt.Errorf("replay key %q: %w", item.key, err); cancel() })
					return
				}
			}
		}()
	}
sendLoop:
	for _, item := range pending {
		select {
		case jobs <- item:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// catchUp consumes all source revisions through 'until', coalesces overwrites
// of the same key, and persists the resulting per-key state before advancing
// the cursor. Revisions are not considered replayed until the destination has
// acknowledged every write in the batch. This is safe because the destination
// is NOT yet serving the migrated slot and epoch changes force SDK rescans.
func catchUp(ctx context.Context, stream grpc.ServerStreamingClient[kvstorev1.WatchResponse], target kvstorev1.KVStoreClient, namespace string, slot uint16, cursor *uint64, until uint64, report *MigrationReport) error {
	if *cursor >= until {
		return nil
	}
	pending := make(map[string]replayMutation)
	progress := *cursor
	var pendingBytes int
	var batchEvents uint64
	var sourceEvents int
	flush := func() error {
		if err := applyReplay(ctx, target, namespace, pending); err != nil {
			return err
		}
		report.Replayed += batchEvents
		report.ReplayWrites += uint64(len(pending))
		*cursor = progress
		pending = make(map[string]replayMutation)
		pendingBytes = 0
		batchEvents = 0
		sourceEvents = 0
		return nil
	}
	for progress < until {
		if err := ctx.Err(); err != nil {
			return err
		}
		event, err := stream.Recv()
		if err != nil {
			return err
		}
		if event.GetRevision() < progress {
			return status.Error(codes.DataLoss, "source changefeed revision moved backwards")
		}
		progress = event.GetRevision()
		sourceEvents++
		if !event.GetCheckpoint() && Slot(namespace, event.GetKey()) == slot {
			key := event.GetKey()
			if old, found := pending[key]; found {
				pendingBytes -= len(old.key) + len(old.value)
			}
			mutation := replayMutation{key: key, deleted: event.GetDeleted(), expiresAtUnixMS: event.GetExpiresAtUnixMs()}
			if !mutation.deleted {
				mutation.value = append([]byte(nil), event.GetValue()...)
			}
			pending[key] = mutation
			pendingBytes += len(key) + len(mutation.value)
			batchEvents++
		}
		if len(pending) >= migrationReplayMaxKeys || pendingBytes >= migrationReplayMaxBytes || sourceEvents >= migrationReplayMaxEvents {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (r *Router) setFence(ctx context.Context, shard int, namespace string, slot uint16, sealed bool) error {
	client, err := r.backend(shard)
	if err != nil {
		return err
	}
	if r.fenceToken == "" {
		return status.Error(codes.FailedPrecondition, "KVSTORE_JOIN_TOKEN required for migration fences")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+r.fenceToken)
	_, err = client.FenceSlot(ctx, &kvstorev1.FenceSlotRequest{Namespace: namespace, Slot: uint32(slot), Sealed: sealed})
	return err
}

// MigrateSlot transfers a single (namespace,hash-slot) while other reads and
// writes continue on the old owner. During the final cutover this slot alone
// is temporarily gated. The source is sealed *inside its replicated Raft FSM*
// to prevent delayed/unknown-outcome writes from arriving after the cutover.
//
// Requirement: exactly one router accepting client traffic, no direct writes
// to the shard backends, and a healthy quorum for source, target and shard 0.
// This initial implementation does not support dynamic group creation or
// migration orchestration across multiple independently writing routers.
func (r *Router) MigrateSlot(ctx context.Context, namespace string, slot uint16, targetShard int) (report MigrationReport, retErr error) {
	if namespace == "" || slot >= SlotCount || targetShard < 0 || targetShard >= ShardCount {
		return report, status.Error(codes.InvalidArgument, "invalid migration namespace, slot or target")
	}
	if err := invalidKey(namespace, "migration"); err != nil {
		return report, err
	}
	if !r.loaded.Load() || r.unresolved.Load() {
		return report, status.Error(codes.FailedPrecondition, "router routing state unavailable")
	}
	if r.fenceToken == "" {
		return report, status.Error(codes.FailedPrecondition, "migration fence token is not configured")
	}
	r.migrationMu.Lock()
	defer r.migrationMu.Unlock()
	if err := r.checkRouteAvailable(); err != nil {
		return report, err
	}
	if err := r.recoverTransactionLocked(ctx); err != nil {
		return report, status.Errorf(codes.Unavailable, "recover outstanding transaction before slot migration: %v", err)
	}
	s := r.current()
	sourceID := s.owner(namespace, slot)
	if sourceID == targetShard {
		return report, status.Error(codes.AlreadyExists, "slot is already assigned to destination")
	}
	report = MigrationReport{Namespace: namespace, Slot: slot, Source: sourceID, Target: targetShard, OldEpoch: s.Epoch}
	source, err := r.backend(sourceID)
	if err != nil {
		return report, err
	}
	destination, err := r.backend(targetShard)
	if err != nil {
		return report, err
	}

	// Source remains authoritative during this long phase. A previous aborted
	// migration may have left sealed state on either cluster; unseal before
	// cleaning the destination and beginning the new snapshot.
	if err = r.setFence(ctx, sourceID, namespace, slot, false); err != nil {
		r.unresolved.Store(true)
		r.ready.Store(false)
		return report, fmt.Errorf("source fence state uncertain; router fenced: %w", err)
	}
	if err = r.setFence(ctx, targetShard, namespace, slot, false); err != nil {
		return report, fmt.Errorf("prepare target fence: %w", err)
	}

	// Remove stale copies of this slot from prior, aborted/reversed migrations.
	// No client can legitimately write this slot on the destination yet.
	_, report.RemovedStale, err = parallelSlotScan(ctx, destination, namespace, slot, 16, func(workCtx context.Context, item *kvstorev1.ScanResponse) error {
		_, e := destination.Delete(workCtx, &kvstorev1.DeleteRequest{Namespace: namespace, Key: item.GetKey()})
		return e
	})
	if err != nil {
		return report, fmt.Errorf("clear destination slot: %w", err)
	}

	checkpoint, copied, err := parallelSlotScan(ctx, source, namespace, slot, 16, func(workCtx context.Context, item *kvstorev1.ScanResponse) error {
		_, e := setOnBackend(workCtx, destination, &kvstorev1.SetRequest{Namespace: namespace, Key: item.GetKey(), Value: item.GetValue(), ExpiresAtUnixMs: item.GetExpiresAtUnixMs()})
		return e
	})
	report.Copied = copied
	if err != nil {
		return report, fmt.Errorf("copy source snapshot: %w", err)
	}

	// The watch starts from the snapshot's *beginning* revision. Source
	// mutations concurrent with the copy are replayed before the cutover.
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	feed, err := source.Watch(watchCtx, &kvstorev1.WatchRequest{Namespace: namespace, AfterRevision: checkpoint})
	if err != nil {
		return report, fmt.Errorf("start source changefeed: %w", err)
	}
	head, err := source.Head(ctx, &kvstorev1.HeadRequest{})
	if err != nil {
		return report, err
	}
	cursor := checkpoint
	if err := catchUp(ctx, feed, destination, namespace, slot, &cursor, head.GetRevision(), &report); err != nil {
		return report, fmt.Errorf("catch up live writes: %w", err)
	}

	gate := r.slotGate(namespace, slot)
	gate.Lock()
	defer gate.Unlock()
	freezeStarted := time.Now()
	// Apply a real cutover deadline to ALL outbound RPCs, not just to
	// the watch stream. Ambiguous Raft outcomes fail closed.
	cutoverCtx, finishCutover := context.WithTimeout(ctx, 20*time.Second)
	defer finishCutover()
	// Bound the slot's write pause. Canceling the source watch interrupts
	// gRPC Recv, and the deferred cleanup unseals the still-current owner.
	freezeTimer := time.AfterFunc(20*time.Second, cancel)
	defer freezeTimer.Stop()
	// Source is still the only write owner. Replicated seal is ordered after
	// all accepted Raft commands on the source. No late command can commit
	// after this seal, including a client call whose outcome was unknown.
	if err := r.setFence(cutoverCtx, sourceID, namespace, slot, true); err != nil {
		r.unresolved.Store(true)
		r.ready.Store(false)
		return report, fmt.Errorf("source seal outcome uncertain; router fenced: %w", err)
	}
	sealed := true
	defer func() {
		if sealed {
			// Never unseal after routing metadata has been durably committed.
			// Earlier aborts restore availability on the still-authoritative source.
			recoverCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
			defer done()
			if e := r.setFence(recoverCtx, sourceID, namespace, slot, false); e != nil {
				r.unresolved.Store(true)
				r.ready.Store(false)
				retErr = errors.Join(retErr, fmt.Errorf("unseal source failed; router fenced: %w", e))
			}
		}
	}()

	finalHead, err := source.Head(cutoverCtx, &kvstorev1.HeadRequest{})
	if err != nil {
		return report, fmt.Errorf("read sealed source head: %w", err)
	}
	if err := catchUp(cutoverCtx, feed, destination, namespace, slot, &cursor, finalHead.GetRevision(), &report); err != nil {
		return report, fmt.Errorf("final source replay: %w", err)
	}

	next := s.copy()
	next.Epoch++
	if next.Epoch == 0 {
		return report, errors.New("routing epoch exhausted")
	}
	if next.Overrides[namespace] == nil {
		next.Overrides[namespace] = make(map[uint16]uint8)
	}
	if int(slot)%ShardCount == targetShard {
		delete(next.Overrides[namespace], slot)
	} else {
		next.Overrides[namespace][slot] = uint8(targetShard)
	}
	if err := r.persistRouting(cutoverCtx, next); err != nil {
		// Outcome-unknown means we do NOT know whether the cutover occurred;
		// leave the old source sealed and fail closed until operator recovery.
		sealed = false
		return report, err
	}
	r.install(next)
	sealed = false // old source must remain sealed after successful cutover
	report.NewEpoch, report.CutoverFreezeMS = next.Epoch, float64(time.Since(freezeStarted).Microseconds())/1000
	return report, nil
}
