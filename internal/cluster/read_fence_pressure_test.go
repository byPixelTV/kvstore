package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Requests arriving AFTER verification has started must get a fresh quorum
// proof. Requests waiting BEFORE the next verification starts can share it.
func TestReadFenceCoalescesWhileVerifierIsBusy(t *testing.T) {
	firstEntered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	stats := &readMetrics{}
	c := newReadFenceCoordinatorWithLimit(0, 1, 1, func() error {
		if calls.Add(1) == 1 {
			close(firstEntered)
			<-release
		}
		return nil
	}, stats)
	defer c.Close()

	const n = 64
	results := make(chan error, n+1)
	go func() { results <- c.Wait(context.Background()) }()
	select {
	case <-firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first quorum verification never started")
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- c.Wait(context.Background())
		}()
	}
	// Wait until ALL 64 subsequent read invocations have registered while
	// the only quorum-verification slot is busy.
	deadline := time.After(3 * time.Second)
	for stats.requests.Load() != n+1 {
		select {
		case <-deadline:
			close(release)
			t.Fatal("waiting reads did not register")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if calls.Load() != 1 {
		close(release)
		t.Fatalf("expected only one active verification, got %d", calls.Load())
	}
	close(release)
	wg.Wait()
	for i := 0; i < n+1; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("fence was not released")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("want 2 verifications (initial and 64 waiter batch), got %d", calls.Load())
	}
	perf := stats.Snapshot()
	if perf.PeakVerifications != 1 || perf.ReadFenceBatches != 2 || perf.StrongReadRequests != n+1 {
		t.Fatalf("unexpected stats: %+v", perf)
	}
}

func TestReadFenceLimitsConcurrentQuorumRPCs(t *testing.T) {
	var active, peak atomic.Int64
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	c := newReadFenceCoordinatorWithLimit(0, 1, 2, func() error {
		a := active.Add(1)
		for {
			old := peak.Load()
			if a <= old || peak.CompareAndSwap(old, a) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		return nil
	}, &readMetrics{})
	defer c.Close()
	results := make(chan error, 3)
	go func() { results <- c.Wait(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("first concurrent check did not start")
	}
	// Start the second read AFTER the first verification has begun.
	go func() { results <- c.Wait(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("second concurrent check did not start")
	}
	go func() { results <- c.Wait(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	if peak.Load() != 2 {
		close(release)
		t.Fatalf("unexpected concurrent checks %d", peak.Load())
	}
	close(release)
	for i := 0; i < 3; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("check hung")
		}
	}
}
