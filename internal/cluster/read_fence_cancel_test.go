package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanceledReadDoesNotCauseExpensiveVerify(t *testing.T) {
	var verifies atomic.Int64
	c := newReadFenceCoordinatorWithLimit(50*time.Millisecond, 8, 1, func() error { verifies.Add(1); return nil }, &readMetrics{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- c.Wait(ctx) }()
	for c.stats.requests.Load() != 1 {
		time.Sleep(100 * time.Microsecond)
	}
	cancel()
	if err := <-result; err != context.Canceled {
		t.Fatalf("unexpected error %v", err)
	}
	c.Close()
	if got := verifies.Load(); got != 0 {
		t.Fatalf("performed %d unnecessary quorum checks", got)
	}
}
