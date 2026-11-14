package main

import (
	"sync/atomic"
	"time"
)

// logicalPacer is a concurrency-safe global *maximum* offered-load limiter.
// A BatchWrite reservation consumes one token per logical write rather than
// one per RPC, so the two modes remain comparable. It does not turn the
// closed-loop benchmark into an open-loop queueing benchmark.
type logicalPacer struct {
	intervalNS int64
	next       atomic.Int64
}

func newLogicalPacer(opsPerSecond int) *logicalPacer {
	if opsPerSecond <= 0 {
		return nil
	}
	interval := int64(time.Second) / int64(opsPerSecond)
	if interval < 1 {
		interval = 1
	}
	return &logicalPacer{intervalNS: interval}
}

func (p *logicalPacer) Wait(weight int, deadline time.Time) bool {
	if p == nil {
		return time.Now().Before(deadline)
	}
	if weight < 1 {
		weight = 1
	}
	for {
		now := time.Now().UnixNano()
		current := p.next.Load()
		start := current
		if start < now {
			start = now
		}
		if !time.Unix(0, start).Before(deadline) {
			return false
		}
		if p.next.CompareAndSwap(current, start+int64(weight)*p.intervalNS) {
			if d := time.Until(time.Unix(0, start)); d > 0 {
				time.Sleep(d)
			}
			return time.Now().Before(deadline)
		}
	}
}
