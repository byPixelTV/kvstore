package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// readFenceCoordinator batches only requests that arrived BEFORE quorum
// verification begins. In particular, reads arriving while an older fence
// is verifying never share its result: that would not be linearizable.
//
// A small semaphore bounds simultaneous VerifyLeader RPCs. While all slots
// are busy, newly arriving reads may share the NEXT pending fence, because
// its verification has not begun yet. maxSize is a dispatch threshold, not a
// hard waiter limit during overload; this is intentional backpressure.
type readFenceCoordinator struct {
	mu      sync.Mutex
	pending *readFenceBatch
	window  time.Duration
	maxSize int
	slots   chan struct{}
	verify  func() error
	closed  bool
	wg      sync.WaitGroup
	stats   *readMetrics
}

type readFenceBatch struct {
	startOnce sync.Once
	done      chan struct{}
	err       error
	waiters   int // guarded by coordinator.mu until detached
	created   time.Time
}

var errReadFenceClosed = errors.New("read fence coordinator closed")

func newReadFenceCoordinator(window time.Duration, maxSize int, verify func() error, stats *readMetrics) *readFenceCoordinator {
	return newReadFenceCoordinatorWithLimit(window, maxSize, 2, verify, stats)
}

func newReadFenceCoordinatorWithLimit(window time.Duration, maxSize, maxConcurrent int, verify func() error, stats *readMetrics) *readFenceCoordinator {
	if maxSize < 1 {
		maxSize = 1
	}
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	if window < 0 {
		window = 0
	}
	return &readFenceCoordinator{
		window: window, maxSize: maxSize, slots: make(chan struct{}, maxConcurrent),
		verify: verify, stats: stats,
	}
}

func (c *readFenceCoordinator) schedule(b *readFenceBatch) {
	b.startOnce.Do(func() { go c.execute(b) })
}

func (c *readFenceCoordinator) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errReadFenceClosed
	}
	b := c.pending
	if b == nil {
		b = &readFenceBatch{done: make(chan struct{}), created: time.Now()}
		c.pending = b
		c.wg.Add(1) // under mutex: Close cannot race this Add
		time.AfterFunc(c.window, func() { c.schedule(b) })
	}
	b.waiters++
	c.stats.requests.Add(1)
	// If all verification slots are occupied, leave this batch pending
	// so additional concurrent readers can share its NEXT quorum check.
	if b.waiters >= c.maxSize {
		c.schedule(b)
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		// The client left before the next verification started: do not
		// inflate its pending batch or spend a quorum roundtrip for it.
		c.mu.Lock()
		if c.pending == b && b.waiters > 0 {
			b.waiters--
		}
		c.mu.Unlock()
		return ctx.Err()
	case <-b.done:
		return b.err
	}
}

func (c *readFenceCoordinator) execute(b *readFenceBatch) {
	// Acquire a slot BEFORE detaching the pending batch. This is the
	// essential linearizability boundary: no request can join after the
	// actual VerifyLeader call starts, and no quorum-RPC stampede occurs.
	c.slots <- struct{}{}
	defer func() { <-c.slots; c.wg.Done() }()

	c.mu.Lock()
	if c.pending == b {
		c.pending = nil
	}
	waiters := b.waiters
	c.mu.Unlock()
	if waiters == 0 {
		close(b.done) // all clients canceled before quorum verification
		return
	}

	queueWait := time.Since(b.created)
	active := c.stats.activeVerifications.Add(1)
	for {
		peak := c.stats.peakVerifications.Load()
		if active <= peak || c.stats.peakVerifications.CompareAndSwap(peak, active) {
			break
		}
	}
	start := time.Now()
	b.err = c.verify()
	c.stats.activeVerifications.Add(-1)
	c.stats.batches.Add(1)
	c.stats.batchWaiters.Add(uint64(waiters))
	c.stats.queueNanos.Add(uint64(queueWait.Nanoseconds()))
	c.stats.fenceNanos.Add(uint64(time.Since(start).Nanoseconds()))
	close(b.done) // publishes b.err to all waiters
}

func (c *readFenceCoordinator) Close() {
	c.mu.Lock()
	c.closed = true
	pending := c.pending
	c.mu.Unlock()
	if pending != nil {
		c.schedule(pending)
	}
	c.wg.Wait()
}

type readMetrics struct {
	requests            atomic.Uint64
	batches             atomic.Uint64
	batchWaiters        atomic.Uint64
	fenceNanos          atomic.Uint64
	queueNanos          atomic.Uint64
	activeVerifications atomic.Int64
	peakVerifications   atomic.Int64
	barrierCalls        atomic.Uint64
	barrierNanos        atomic.Uint64
	verifyCalls         atomic.Uint64
	verifyNanos         atomic.Uint64
	localReads          atomic.Uint64
	localNanos          atomic.Uint64
	forwardReads        atomic.Uint64
	forwardNanos        atomic.Uint64
	eventualReads       atomic.Uint64
}

type ReadPerformance struct {
	StrongReadRequests    uint64  `json:"strong_read_requests"`
	ReadFenceBatches      uint64  `json:"read_fence_batches"`
	AverageBatchSize      float64 `json:"average_batch_size"`
	AverageFenceMS        float64 `json:"average_fence_ms"`
	AverageFenceQueueMS   float64 `json:"average_fence_queue_ms"`
	ActiveVerifications   int64   `json:"active_verifications"`
	PeakVerifications     int64   `json:"peak_verifications"`
	BarrierCalls          uint64  `json:"barrier_calls"`
	AverageBarrierMS      float64 `json:"average_barrier_ms"`
	VerifyLeaderCalls     uint64  `json:"verify_leader_calls"`
	AverageVerifyLeaderMS float64 `json:"average_verify_leader_ms"`
	LocalReads            uint64  `json:"local_reads"`
	AverageLocalReadMS    float64 `json:"average_local_read_ms"`
	ForwardedReads        uint64  `json:"forwarded_reads"`
	AverageForwardedMS    float64 `json:"average_forwarded_ms"`
	EventualReads         uint64  `json:"eventual_reads"`
}

func average(n, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(n) / float64(denominator)
}

func (m *readMetrics) Snapshot() ReadPerformance {
	batchCount := m.batches.Load()
	barrierCount := m.barrierCalls.Load()
	verifyCount := m.verifyCalls.Load()
	localCount := m.localReads.Load()
	forwardCount := m.forwardReads.Load()
	return ReadPerformance{
		StrongReadRequests:    m.requests.Load(),
		ReadFenceBatches:      batchCount,
		AverageBatchSize:      average(m.batchWaiters.Load(), batchCount),
		AverageFenceMS:        average(m.fenceNanos.Load(), batchCount) / 1e6,
		AverageFenceQueueMS:   average(m.queueNanos.Load(), batchCount) / 1e6,
		ActiveVerifications:   m.activeVerifications.Load(),
		PeakVerifications:     m.peakVerifications.Load(),
		BarrierCalls:          barrierCount,
		AverageBarrierMS:      average(m.barrierNanos.Load(), barrierCount) / 1e6,
		VerifyLeaderCalls:     verifyCount,
		AverageVerifyLeaderMS: average(m.verifyNanos.Load(), verifyCount) / 1e6,
		LocalReads:            localCount,
		AverageLocalReadMS:    average(m.localNanos.Load(), localCount) / 1e6,
		ForwardedReads:        forwardCount,
		AverageForwardedMS:    average(m.forwardNanos.Load(), forwardCount) / 1e6,
		EventualReads:         m.eventualReads.Load(),
	}
}
