package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	clusterWriteBatchWindow   = 1500 * time.Microsecond
	clusterWriteBatchMax      = 256
	clusterWriteQueueCapacity = 8192
	// A 256-KiB value fits, including JSON/base64 overhead. Never enlarge
	// a batch beyond this budget just because the command count allows it.
	clusterWriteBatchBytes  = 400 << 10
	maxRaftCommandsPerBatch = 256
	defaultRaftInflight     = 8
)

var errClusterBatchClosed = errors.New("cluster write batcher is closed")

type clusterWriteRequest struct {
	ctx      context.Context
	queuedAt time.Time
	cmd      command
	// batch is non-nil for a client BatchWrite RPC. Keep it as ONE Raft
	// command so a migration fence cannot reject unrelated queued writes.
	batch      []command
	reply      chan writeResponse
	batchReply chan batchWriteResponse
}

type batchWriteResponse struct {
	revisions []uint64
	err       error
}

func (r clusterWriteRequest) isBatch() bool { return r.batch != nil }

type WritePerformance struct {
	Submitted          uint64  `json:"submitted_writes"`
	Groups             uint64  `json:"submitted_raft_groups"`
	Completed          uint64  `json:"completed_raft_groups"`
	GroupErrors        uint64  `json:"failed_raft_groups"`
	AverageBatch       float64 `json:"average_batch_size"`
	AverageApplyMS     float64 `json:"average_raft_apply_ms"`
	AverageAdmissionMS float64 `json:"average_raft_admission_ms"`
	QueueDepth         int     `json:"queue_depth"`
	AverageQueueMS     float64 `json:"average_queue_wait_ms"`
	InFlight           int64   `json:"in_flight"`
	PeakInFlight       int64   `json:"peak_in_flight"`
	BatchRPCs          uint64  `json:"submitted_batch_rpcs"`
	BatchLogicalWrites uint64  `json:"submitted_batch_logical_writes"`
	MaxInFlight        int     `json:"configured_max_in_flight"`
	BatchTarget        int     `json:"configured_batch_target"`
	BatchWindowMS      float64 `json:"configured_batch_window_ms"`
	BatchMaxWindowMS   float64 `json:"configured_batch_max_window_ms"`
}

type writeCounters struct {
	submitted      atomic.Uint64
	groups         atomic.Uint64
	logical        atomic.Uint64
	completed      atomic.Uint64
	failed         atomic.Uint64
	applyNanos     atomic.Uint64
	admissionNanos atomic.Uint64
	queueNanos     atomic.Uint64
	queuedCount    atomic.Uint64
	batchRPCs      atomic.Uint64
	batchLogical   atomic.Uint64
	inFlight       atomic.Int64
	peak           atomic.Int64
}

type clusterWriteBatcher struct {
	node           *Node
	gate           sync.RWMutex
	closed         bool
	queue          chan clusterWriteRequest
	done           chan struct{}
	batchWindow    time.Duration
	batchMaxWindow time.Duration
	batchTarget    int
	batchMax       int
	maxInflight    int
	stats          writeCounters
}

func newClusterWriteBatcher(node *Node) *clusterWriteBatcher {
	window := node.cfg.WriteBatchWindow
	if window <= 0 {
		window = clusterWriteBatchWindow
	}
	maxBatch := node.cfg.WriteBatchMax
	if maxBatch <= 0 {
		maxBatch = clusterWriteBatchMax
	}
	if maxBatch > maxRaftCommandsPerBatch {
		maxBatch = maxRaftCommandsPerBatch
	}
	target := node.cfg.WriteBatchTarget
	if target <= 0 {
		target = 24
	}
	if target > maxBatch {
		target = maxBatch
	}
	maxWindow := node.cfg.WriteBatchMaxWindow
	if maxWindow <= 0 {
		maxWindow = 5 * time.Millisecond
	}
	if maxWindow < window {
		maxWindow = window
	}
	inflight := node.cfg.RaftMaxInflight
	if inflight <= 0 {
		inflight = defaultRaftInflight
	}
	b := &clusterWriteBatcher{
		node: node, queue: make(chan clusterWriteRequest, clusterWriteQueueCapacity),
		done: make(chan struct{}), batchWindow: window, batchMaxWindow: maxWindow, batchTarget: target, batchMax: maxBatch,
		maxInflight: inflight,
	}
	go b.loop()
	return b
}

func (b *clusterWriteBatcher) Performance() WritePerformance {
	count := b.stats.completed.Load()
	groups := b.stats.groups.Load()
	perf := WritePerformance{
		Submitted: b.stats.submitted.Load(), Groups: groups,
		Completed: count, GroupErrors: b.stats.failed.Load(),
		InFlight: b.stats.inFlight.Load(), PeakInFlight: b.stats.peak.Load(),
		MaxInFlight: b.maxInflight, QueueDepth: len(b.queue),
		BatchRPCs: b.stats.batchRPCs.Load(), BatchLogicalWrites: b.stats.batchLogical.Load(),
		BatchTarget: b.batchTarget, BatchWindowMS: float64(b.batchWindow) / 1e6,
		BatchMaxWindowMS: float64(b.batchMaxWindow) / 1e6,
	}
	if groups > 0 {
		perf.AverageBatch = float64(b.stats.logical.Load()) / float64(groups)
	}
	if count > 0 {
		perf.AverageApplyMS = float64(b.stats.applyNanos.Load()) / float64(count) / 1e6
	}
	if groups > 0 {
		perf.AverageAdmissionMS = float64(b.stats.admissionNanos.Load()) / float64(groups) / 1e6
	}
	if n := b.stats.queuedCount.Load(); n > 0 {
		perf.AverageQueueMS = float64(b.stats.queueNanos.Load()) / float64(n) / 1e6
	}
	return perf
}

func (b *clusterWriteBatcher) Submit(ctx context.Context, cmd command) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	cmd.Value = bytes.Clone(cmd.Value)
	req := clusterWriteRequest{ctx: ctx, queuedAt: time.Now(), cmd: cmd, reply: make(chan writeResponse, 1)}
	b.gate.RLock()
	if b.closed {
		b.gate.RUnlock()
		return 0, errClusterBatchClosed
	}
	select {
	case b.queue <- req:
		b.stats.submitted.Add(1)
		b.gate.RUnlock()
	case <-ctx.Done():
		b.gate.RUnlock()
		return 0, ctx.Err()
	}
	select {
	case res := <-req.reply:
		return res.revision, res.err
	case <-ctx.Done():
		// May have committed despite cancellation; callers must treat this
		// result as outcome-unknown and must not blindly retry increments.
		return 0, ctx.Err()
	}
}

// SubmitBatch sends one atomic batch through the SAME bounded pipeline as
// ordinary SET and DELETE calls. It never mixes a user batch with unrelated
// requests: a replicated migration fence must not reject other clients'
// operations merely because they shared a physical Raft command.
// An RPC timeout after admission has UNKNOWN commit outcome; no retry.
func (b *clusterWriteBatcher) SubmitBatch(ctx context.Context, commands []command) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(commands) == 0 || len(commands) > 128 {
		return nil, errors.New("invalid batch length")
	}
	copyCommands := make([]command, len(commands))
	copy(copyCommands, commands)
	for i := range copyCommands {
		copyCommands[i].Value = bytes.Clone(copyCommands[i].Value)
	}
	req := clusterWriteRequest{ctx: ctx, queuedAt: time.Now(), batch: copyCommands, batchReply: make(chan batchWriteResponse, 1)}
	b.gate.RLock()
	if b.closed {
		b.gate.RUnlock()
		return nil, errClusterBatchClosed
	}
	select {
	case b.queue <- req:
		b.stats.submitted.Add(uint64(len(commands)))
		b.stats.batchRPCs.Add(1)
		b.stats.batchLogical.Add(uint64(len(commands)))
		b.gate.RUnlock()
	case <-ctx.Done():
		b.gate.RUnlock()
		return nil, ctx.Err()
	}
	select {
	case res := <-req.batchReply:
		return res.revisions, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *clusterWriteBatcher) Close() {
	b.gate.Lock()
	if !b.closed {
		b.closed = true
		close(b.queue)
	}
	b.gate.Unlock()
	<-b.done
}

func estimateRaftCommand(c command) int {
	return 128 + len(c.Namespace) + len(c.Key) + 4*(len(c.Value)+2)/3
}

func (b *clusterWriteBatcher) loop() {
	slots := make(chan struct{}, b.maxInflight)
	var pending *clusterWriteRequest
	var wg sync.WaitGroup
	defer func() { wg.Wait(); close(b.done) }()

	for {
		var first clusterWriteRequest
		if pending != nil {
			first = *pending
			pending = nil
		} else {
			next, ok := <-b.queue
			if !ok {
				return
			}
			first = next
		}
		// First reserve capacity. Under disk contention the queue builds up
		// and the next batch can use that backlog efficiently.
		slots <- struct{}{}
		group := []clusterWriteRequest{first}
		queueClosed := false
		if !first.isBatch() {
			totalBytes := estimateRaftCommand(first.cmd)
			window := batchCollectionWindow(b.batchWindow, b.batchMaxWindow, len(slots))
			timer := time.NewTimer(window)
		collect:
			for len(group) < b.batchMax {
				select {
				case next, ok := <-b.queue:
					if !ok {
						queueClosed = true
						break collect
					}
					// A client BatchWrite is an indivisible Raft command,
					// including with respect to migration fences.
					if next.isBatch() {
						pending = &next
						break collect
					}
					estimated := estimateRaftCommand(next.cmd)
					if totalBytes+estimated > clusterWriteBatchBytes {
						pending = &next
						break collect
					}
					group = append(group, next)
					totalBytes += estimated
					if len(group) >= b.batchTarget {
						break collect
					}
				case <-timer.C:
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		b.dispatch(group, slots, &wg)
		if queueClosed && pending == nil {
			return
		}
	}
}

func (b *clusterWriteBatcher) dispatch(group []clusterWriteRequest, slots chan struct{}, wg *sync.WaitGroup) {
	isBatch := group[0].isBatch()
	results := make([]writeResponse, len(group))
	commands := make([]command, 0, len(group))
	positions := make([]int, 0, len(group))
	for i, req := range group {
		b.stats.queueNanos.Add(uint64(time.Since(req.queuedAt).Nanoseconds()))
		b.stats.queuedCount.Add(1)
		if err := req.ctx.Err(); err != nil {
			results[i].err = err
			continue
		}
		if isBatch {
			commands = append(commands, req.batch...)
		} else {
			commands = append(commands, req.cmd)
			positions = append(positions, i)
		}
	}
	var batchResult batchWriteResponse
	replyAll := func() {
		if isBatch {
			group[0].batchReply <- batchResult
			return
		}
		for i, req := range group {
			req.reply <- results[i]
		}
	}
	if len(commands) == 0 {
		if isBatch {
			batchResult.err = group[0].ctx.Err()
		}
		replyAll()
		<-slots
		return
	}

	cmd := commands[0]
	if isBatch {
		cmd = command{Operation: opBatch, Commands: commands}
	} else if len(commands) > 1 {
		cmd = command{Operation: opMicrobatch, Commands: commands}
	}
	raw, err := json.Marshal(cmd)
	if err == nil && len(raw) > clusterWriteBatchBytes {
		err = fmt.Errorf("raft command too large (%d bytes)", len(raw))
	}
	if err != nil {
		if isBatch {
			batchResult.err = err
		} else {
			for _, i := range positions {
				results[i].err = err
			}
		}
		replyAll()
		<-slots
		return
	}

	// The submission loop is the only writer into raft.Apply. This keeps
	// the log append order deterministic while futures complete in parallel.
	started := time.Now()
	future := b.node.raft.Apply(raw, opTimeout)
	b.stats.admissionNanos.Add(uint64(time.Since(started).Nanoseconds()))
	b.stats.groups.Add(1)
	b.stats.logical.Add(uint64(len(commands)))
	active := b.stats.inFlight.Add(1)
	for {
		peak := b.stats.peak.Load()
		if active <= peak || b.stats.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { b.stats.inFlight.Add(-1); <-slots }()
		// A Raft timeout does not imply that a write was rolled back.
		outcome := make(chan error, 1)
		go func() { outcome <- future.Error() }()
		var applyErr error
		select {
		case applyErr = <-outcome:
		case <-time.After(opTimeout):
			applyErr = context.DeadlineExceeded
		}
		b.stats.applyNanos.Add(uint64(time.Since(started).Nanoseconds()))
		b.stats.completed.Add(1)
		if applyErr == nil {
			response, ok := future.Response().(applyResult)
			if !ok {
				applyErr = fmt.Errorf("unexpected FSM response: %T", future.Response())
			} else if response.Err != nil {
				applyErr = response.Err
			} else if isBatch {
				if len(response.Revisions) != len(commands) {
					applyErr = fmt.Errorf("raft batch returned %d revisions for %d commands", len(response.Revisions), len(commands))
				} else {
					batchResult.revisions = response.Revisions
				}
			} else if len(commands) == 1 {
				results[positions[0]].revision = response.Revision
			} else if len(response.Revisions) != len(commands) {
				applyErr = fmt.Errorf("raft batch returned %d revisions for %d commands", len(response.Revisions), len(commands))
			} else {
				for j, i := range positions {
					results[i].revision = response.Revisions[j]
					if len(response.Errors) == len(commands) {
						results[i].err = response.Errors[j]
						if results[i].err != nil {
							b.stats.failed.Add(1)
						}
					}
				}
			}
		}
		if applyErr != nil {
			b.stats.failed.Add(1)
			if isBatch {
				batchResult.err = applyErr
			} else {
				for _, i := range positions {
					results[i].err = applyErr
				}
			}
		}
		replyAll()
	}()
}
