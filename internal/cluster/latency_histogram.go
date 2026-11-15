package cluster

import (
	"sync/atomic"
	"time"
)

// latencyHistogram is a fixed-memory, lock-free upper-bound histogram. It
// measures the actual cost of Raft admission and apply (and queue time), not
// the time spent in a client gRPC call. Sample cost is one atomic increment.
// Quantiles are intentionally conservative bucket upper bounds.
var latencyBucketUpper = [...]time.Duration{
	250 * time.Microsecond, 500 * time.Microsecond,
	time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond,
	8 * time.Millisecond, 12 * time.Millisecond, 16 * time.Millisecond,
	24 * time.Millisecond, 32 * time.Millisecond, 48 * time.Millisecond,
	64 * time.Millisecond, 96 * time.Millisecond, 128 * time.Millisecond,
	192 * time.Millisecond, 256 * time.Millisecond, 384 * time.Millisecond,
	512 * time.Millisecond, 768 * time.Millisecond, time.Second,
	2 * time.Second, 4 * time.Second, 8 * time.Second,
	16 * time.Second,
}

type latencyHistogram struct {
	buckets [len(latencyBucketUpper) + 1]atomic.Uint64
}

func (h *latencyHistogram) Observe(elapsed time.Duration) {
	if elapsed < 0 {
		elapsed = 0
	}
	for i, upper := range latencyBucketUpper {
		if elapsed <= upper {
			h.buckets[i].Add(1)
			return
		}
	}
	h.buckets[len(latencyBucketUpper)].Add(1)
}

func (h *latencyHistogram) P99MS() float64 {
	var total uint64
	for i := range h.buckets {
		total += h.buckets[i].Load()
	}
	if total == 0 {
		return 0
	}
	// ceil(0.99 * n), using integers (avoid precision loss).
	target := (total*99 + 99) / 100
	var cumulative uint64
	for i := range h.buckets {
		cumulative += h.buckets[i].Load()
		if cumulative >= target {
			if i == len(latencyBucketUpper) {
				// Open-ended overflow bucket: 16s is only a lower bound.
				// Do not lie by reporting an upper bound for it.
				return -1
			}
			return float64(latencyBucketUpper[i]) / float64(time.Millisecond)
		}
	}
	return -1
}
