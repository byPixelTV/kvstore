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
	reply    chan writeResponse
}

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
		// Reserve the next Raft pipeline slot BEFORE starting the collection
		// window. When the pipeline is saturated, waiting for a slot lets
		// requests accumulate in the bounded queue. The next batch drains
		// that backlog rather than being frozen at a small batch size while
		// waiting for an in-flight future to finish.
		slots <- struct{}{}
		group := []clusterWriteRequest{first}
		totalBytes := estimateRaftCommand(first.cmd)
		// Under a saturated I/O pipeline, a few extra milliseconds of
		// batching can amortize the Raft log-store sync and the Pebble
		// FSM sync across more logical writes. Stop early once target is
		// reached so a large backlog does NOT pay the full extra delay.
		// A lightly loaded cluster retains the low-latency base window.
		window := batchCollectionWindow(b.batchWindow, b.batchMaxWindow, len(slots))

		timer := time.NewTimer(window)
		queueClosed := false
	collect:
		for len(group) < b.batchMax {
			select {
			case next, ok := <-b.queue:
				if !ok {
					queueClosed = true
					break collect
				}
				estimated := estimateRaftCommand(next.cmd)
				if totalBytes+estimated > clusterWriteBatchBytes {
					pending = &next
					break collect
				}
				group = append(group, next)
				totalBytes += estimated
				// We already have enough writes to amortize one durable
				// commit. No need to hold these clients for the whole timer.
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

		// The old implementation blocked here until this whole group had
		// committed. That kept Raft at ONE outstanding command regardless of
		// concurrency. Keep submission ordered, but await futures concurrently.
		b.dispatch(group, slots, &wg)
		if queueClosed && pending == nil {
			return
		}
	}
}

func (b *clusterWriteBatcher) dispatch(group []clusterWriteRequest, slots chan struct{}, wg *sync.WaitGroup) {
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
		commands = append(commands, req.cmd)
		positions = append(positions, i)
	}
	replyAll := func() {
		for i, req := range group {
			req.reply <- results[i]
		}
	}
	if len(commands) == 0 {
		replyAll()
		<-slots
		return
	}

	cmd := commands[0]
	if len(commands) > 1 {
		cmd = command{Operation: opBatch, Commands: commands}
	}
	raw, err := json.Marshal(cmd)
	if err != nil {
		for _, i := range positions {
			results[i].err = err
		}
		replyAll()
		<-slots
		return
	}
	// The maxBytes estimate is conservative, but enforce the real encoded
	// payload limit too: callers get an error, never a silently truncated batch.
	if len(raw) > clusterWriteBatchBytes {
		err = fmt.Errorf("raft command too large (%d bytes)", len(raw))
		for _, i := range positions {
			results[i].err = err
		}
		replyAll()
		<-slots
		return
	}

	// Bound outstanding Raft commands and goroutines under overload.
	// The loop already reserved a slot. Only it submits to raft.Apply,
	// preserving log submission order despite asynchronous completion.
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
		// Future.Error() may remain unresolved when quorum disappears. Cap
		// client wait, matching the original applyDirect() 10s timeout.
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
			} else if len(commands) == 1 {
				results[positions[0]].revision = response.Revision
			} else if len(response.Revisions) != len(commands) {
				applyErr = fmt.Errorf("raft batch returned %d revisions for %d commands", len(response.Revisions), len(commands))
			} else {
				for j, i := range positions {
					results[i].revision = response.Revisions[j]
				}
			}
		}
		if applyErr != nil {
			b.stats.failed.Add(1)
			for _, i := range positions {
				results[i].err = applyErr
			}
		}
		replyAll()
	}()
}
