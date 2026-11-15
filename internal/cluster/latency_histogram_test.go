package cluster

import (
	"sync"
	"testing"
	"time"
)

func TestLatencyHistogramEmptyAndQuantile(t *testing.T) {
	var h latencyHistogram
	if got := h.P99MS(); got != 0 {
		t.Fatalf("empty p99 = %v", got)
	}
	for i := 0; i < 99; i++ {
		h.Observe(1500 * time.Microsecond)
	}
	h.Observe(20 * time.Millisecond)
	if got := h.P99MS(); got != 2 {
		t.Fatalf("99th percentile expected upper bound 2ms, got %v", got)
	}
	h.Observe(21 * time.Millisecond)
	if got := h.P99MS(); got != 24 {
		t.Fatalf("99th percentile expected 24ms with 101 observations, got %v", got)
	}
}

func TestLatencyHistogramConcurrent(t *testing.T) {
	var h latencyHistogram
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h.Observe(40 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if got := h.P99MS(); got != 48 {
		t.Fatalf("concurrent p99=%v", got)
	}
}
