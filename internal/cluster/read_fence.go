package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// readFenceCoordinator batches only requests that arrived BEFORE the fence
// starts. Joining an already in-flight quorum verification would be unsafe:
// it could have proven leadership before the later read was invoked.
//
// Each batch performs one ordered barrier + quorum verification, then every
// waiter in the batch may read its own key from the committed local FSM.
// This avoids one Raft LogBarrier per incoming GET.
type readFenceCoordinator struct {
	mu      sync.Mutex
	pending *readFenceBatch
	window  time.Duration
	maxSize int
	verify  func() error
	closed  bool
	wg      sync.WaitGroup
	stats   *readMetrics
}

type readFenceBatch struct {
	once    sync.Once
	done    chan struct{}
	err     error
	waiters int
}

var errReadFenceClosed = errors.New("read fence coordinator closed")

func newReadFenceCoordinator(window time.Duration, maxSize int, verify func() error, stats *readMetrics) *readFenceCoordinator {
	if maxSize < 1 {
		maxSize = 1
	}
	if window < 0 {
		window = 0
	}
	return &readFenceCoordinator{window: window, maxSize: maxSize, verify: verify, stats: stats}
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
		b = &readFenceBatch{done: make(chan struct{})}
		c.pending = b
		c.wg.Add(1) // under mutex: Close cannot start waiting until Add finishes
		// Timer may fire immediately but execute waits for the same mutex.
		time.AfterFunc(c.window, func() { c.execute(b) })
	}
	b.waiters++
	c.stats.requests.Add(1)
	// When full, dispatch immediately rather than waiting for the timer.
	// sync.Once prevents the timer from running the same batch twice.
	if b.waiters >= c.maxSize {
		go c.execute(b)
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return b.err
	}
}

func (c *readFenceCoordinator) execute(b *readFenceBatch) {
	b.once.Do(func() {
		c.mu.Lock()
		if c.pending == b {
			c.pending = nil
		}
		c.mu.Unlock()
		start := time.Now()
		b.err = c.verify()
		c.stats.batches.Add(1)
		c.stats.batchWaiters.Add(uint64(b.waiters))
		c.stats.fenceNanos.Add(uint64(time.Since(start).Nanoseconds()))
		close(b.done) // publishes err to all waiters
		c.wg.Done()
	})
}

func (c *readFenceCoordinator) Close() {
	c.mu.Lock()
	c.closed = true
	pending := c.pending
	c.mu.Unlock()
	// The Raft instance should already be shut down; its pending futures
	// complete with ErrRaftShutdown. Drain a scheduled batch immediately.
	if pending != nil {
		go c.execute(pending)
	}
	c.wg.Wait()
}

// Cumulative counters, made accessible only via the authenticated admin API.
// Read and update operations never block on these counters.
type readMetrics struct {
	requests      atomic.Uint64
	batches       atomic.Uint64
	batchWaiters  atomic.Uint64
	fenceNanos    atomic.Uint64
	barrierCalls  atomic.Uint64
	barrierNanos  atomic.Uint64
	verifyCalls   atomic.Uint64
	verifyNanos   atomic.Uint64
	localReads    atomic.Uint64
	localNanos    atomic.Uint64
	forwardReads  atomic.Uint64
	forwardNanos  atomic.Uint64
	eventualReads atomic.Uint64
}

type ReadPerformance struct {
	StrongReadRequests    uint64  `json:"strong_read_requests"`
	ReadFenceBatches      uint64  `json:"read_fence_batches"`
	AverageBatchSize      float64 `json:"average_batch_size"`
	AverageFenceMS        float64 `json:"average_fence_ms"`
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
