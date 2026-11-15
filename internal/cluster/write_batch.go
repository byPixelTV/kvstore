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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	clusterWriteBatchWindow   = 1500 * time.Microsecond
	clusterWriteBatchMax      = 256
	clusterWriteQueueCapacity = 2048
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
	P99QueueMS         float64 `json:"p99_queue_wait_ms_upper"`
	P99AdmissionMS     float64 `json:"p99_raft_admission_ms_upper"`
	P99ApplyMS         float64 `json:"p99_raft_apply_ms_upper"`
	InFlight           int64   `json:"in_flight"`
	PeakInFlight       int64   `json:"peak_in_flight"`
	BatchRPCs          uint64  `json:"submitted_batch_rpcs"`
	BatchLogicalWrites uint64  `json:"submitted_batch_logical_writes"`
	RejectedBeforeRaft uint64  `json:"rejected_before_raft"`
	QueueBudgetMS      float64 `json:"configured_queue_budget_ms"`
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
	rejected       atomic.Uint64
	inFlight       atomic.Int64
	peak           atomic.Int64
	queueLatency   latencyHistogram
	admitLatency   latencyHistogram
	applyLatency   latencyHistogram
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
	maxQueueDelay  time.Duration
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
		target = 64
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
		maxInflight: inflight, maxQueueDelay: node.cfg.WriteMaxQueueDelay,
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
		RejectedBeforeRaft: b.stats.rejected.Load(), QueueBudgetMS: float64(b.maxQueueDelay) / 1e6,
		BatchTarget: b.batchTarget, BatchWindowMS: float64(b.batchWindow) / 1e6,
		BatchMaxWindowMS: float64(b.batchMaxWindow) / 1e6,
		P99QueueMS:       b.stats.queueLatency.P99MS(),
		P99AdmissionMS:   b.stats.admitLatency.P99MS(),
		P99ApplyMS:       b.stats.applyLatency.P99MS(),
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

// estimateRaftCommand slightly overestimates JSON/base64 wire size. The
// serialized command is also checked before submitting it to Raft.
func estimateRaftCommand(c command) int {
	return 128 + len(c.Namespace) + len(c.Key) + 4*(len(c.Value)+2)/3
}

// An atomic client BatchWrite counts as a SINGLE admission item but can
// contain up to 128 logical writes. This distinction is why the old code
// emitted many ~5-write Raft logs on a three-shard workload.
func requestLogicalCount(r clusterWriteRequest) int {
	if r.isBatch() {
		return len(r.batch)
	}
	return 1
}

func estimateRaftRequest(r clusterWriteRequest) int {
	if !r.isBatch() {
		return estimateRaftCommand(r.cmd)
	}
	n := 160 // nested opBatch envelope + JSON framing
	for _, c := range r.batch {
		n += estimateRaftCommand(c)
	}
	return n
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
		// This is the admission boundary for a Raft batch. Collect AFTER a
		// slot is available, to exploit backlog when the disk is busy.
		slots <- struct{}{}
		group := []clusterWriteRequest{first}
		logicalCount := requestLogicalCount(first)
		totalBytes := estimateRaftRequest(first)
		closed := false
		window := batchCollectionWindow(b.batchWindow, b.batchMaxWindow, len(slots))
		timer := time.NewTimer(window)
	collect:
		for logicalCount < b.batchTarget && logicalCount < b.batchMax && logicalCount < maxRaftCommandsPerBatch {
			select {
			case next, ok := <-b.queue:
				if !ok {
					closed = true
					break collect
				}
				n := requestLogicalCount(next)
				size := estimateRaftRequest(next)
				if logicalCount+n > b.batchMax || logicalCount+n > maxRaftCommandsPerBatch || totalBytes+size > clusterWriteBatchBytes {
					// Atomic user batches must never be split. Carry the
					// entire next RPC into the next Raft admission window.
					pending = &next
					break collect
				}
				group = append(group, next)
				logicalCount += n
				totalBytes += size
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
		b.dispatch(group, slots, &wg)
		if closed && pending == nil {
			return
		}
	}
}

func (b *clusterWriteBatcher) dispatch(group []clusterWriteRequest, slots chan struct{}, wg *sync.WaitGroup) {
	// The position map associates one physical Raft child with exactly one
	// original RPC. For BatchWrite, that child is an indivisible opBatch.
	type submitted struct {
		req   clusterWriteRequest
		index int
	}
	results := make([]writeResponse, len(group))
	batchResults := make([]batchWriteResponse, len(group))
	valid := make([]submitted, 0, len(group))
	commands := make([]command, 0, len(group))
	replyAll := func() {
		for i, req := range group {
			if req.isBatch() {
				req.batchReply <- batchResults[i]
			} else {
				req.reply <- results[i]
			}
		}
	}
	for i, req := range group {
		wait := time.Since(req.queuedAt)
		b.stats.queueNanos.Add(uint64(wait.Nanoseconds()))
		b.stats.queueLatency.Observe(wait)
		b.stats.queuedCount.Add(1)
		if err := req.ctx.Err(); err != nil {
			if req.isBatch() {
				batchResults[i].err = err
			} else {
				results[i].err = err
			}
			continue
		}
		if b.maxQueueDelay > 0 && wait > b.maxQueueDelay {
			// Not submitted to Raft: this is a DEFINITIVE rejection, not
			// an ambiguous write timeout. Clients may retry with backoff.
			err := status.Error(codes.ResourceExhausted, "raft write queue saturated before admission")
			b.stats.rejected.Add(uint64(requestLogicalCount(req)))
			if req.isBatch() {
				batchResults[i].err = err
			} else {
				results[i].err = err
			}
			continue
		}
		if req.isBatch() {
			commands = append(commands, command{Operation: opBatch, Commands: req.batch})
		} else {
			commands = append(commands, req.cmd)
		}
		valid = append(valid, submitted{req: req, index: i})
	}
	if len(commands) == 0 {
		replyAll()
		<-slots
		return
	}

	// A single client batch retains its original wire representation. Two
	// or more admitted RPCs use opMicrobatch with nested opBatch children,
	// so EACH BatchWrite remains atomic and point-writes stay independent.
	cmd := commands[0]
	if len(commands) > 1 {
		cmd = command{Operation: opMicrobatch, Commands: commands}
	}
	raw, err := json.Marshal(cmd)
	if err == nil && len(raw) > clusterWriteBatchBytes {
		err = fmt.Errorf("raft command too large (%d bytes)", len(raw))
	}
	if err != nil {
		for _, item := range valid {
			if item.req.isBatch() {
				batchResults[item.index].err = err
			} else {
				results[item.index].err = err
			}
		}
		replyAll()
		<-slots
		return
	}

	// Only this submission loop invokes Raft.Apply, keeping log order
	// deterministic. Futures may finish concurrently; results remain mapped
	// to their original RPCs.
	started := time.Now()
	future := b.node.raft.Apply(raw, opTimeout)
	admitDuration := time.Since(started)
	b.stats.admissionNanos.Add(uint64(admitDuration.Nanoseconds()))
	b.stats.admitLatency.Observe(admitDuration)
	b.stats.groups.Add(1)
	logical := uint64(0)
	for _, item := range valid {
		logical += uint64(requestLogicalCount(item.req))
	}
	b.stats.logical.Add(logical)
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
		outcome := make(chan error, 1)
		go func() { outcome <- future.Error() }()
		var applyErr error
		select {
		case applyErr = <-outcome:
		case <-time.After(opTimeout):
			// A Raft timeout is UNKNOWN OUTCOME, never a safe retry signal.
			applyErr = context.DeadlineExceeded
		}
		applyDuration := time.Since(started)
		b.stats.applyNanos.Add(uint64(applyDuration.Nanoseconds()))
		b.stats.applyLatency.Observe(applyDuration)
		b.stats.completed.Add(1)
		if applyErr == nil {
			response, ok := future.Response().(applyResult)
			if !ok {
				applyErr = fmt.Errorf("unexpected FSM response: %T", future.Response())
			} else if response.Err != nil {
				applyErr = response.Err
			} else if len(valid) == 1 {
				item := valid[0]
				if item.req.isBatch() {
					if len(response.Revisions) != len(item.req.batch) {
						applyErr = fmt.Errorf("raft batch returned %d revisions for %d commands", len(response.Revisions), len(item.req.batch))
					} else {
						batchResults[item.index].revisions = response.Revisions
					}
				} else {
					results[item.index].revision = response.Revision
				}
			} else if len(response.Revisions) != len(valid) || len(response.Errors) != len(valid) || len(response.BatchRevisions) != len(valid) {
				applyErr = fmt.Errorf("microbatch returned incorrect item count")
			} else {
				for j, item := range valid {
					itemErr := response.Errors[j]
					if item.req.isBatch() {
						if itemErr == nil && len(response.BatchRevisions[j]) != len(item.req.batch) {
							itemErr = fmt.Errorf("raft nested batch returned incorrect revision count")
						}
						batchResults[item.index].err = itemErr
						if itemErr == nil {
							batchResults[item.index].revisions = response.BatchRevisions[j]
						}
					} else {
						results[item.index].revision = response.Revisions[j]
						results[item.index].err = itemErr
					}
					if itemErr != nil {
						b.stats.failed.Add(1)
					}
				}
			}
		}
		if applyErr != nil {
			b.stats.failed.Add(1)
			for _, item := range valid {
				if item.req.isBatch() {
					batchResults[item.index].err = applyErr
				} else {
					results[item.index].err = applyErr
				}
			}
		}
		replyAll()
	}()
}
