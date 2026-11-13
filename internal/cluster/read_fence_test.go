package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadFenceBatchesConcurrentReads(t *testing.T) {
	const readers = 32
	var calls atomic.Int64
	stats := &readMetrics{}
	coordinator := newReadFenceCoordinator(2*time.Second, readers, func() error {
		calls.Add(1)
		return nil
	}, stats)
	defer coordinator.Close()

	var wg sync.WaitGroup
	start := make(chan struct{})
	failures := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			failures <- coordinator.Wait(context.Background())
		}()
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one shared Raft fence for %d readers, got %d", readers, got)
	}
	snap := stats.Snapshot()
	if snap.StrongReadRequests != readers || snap.ReadFenceBatches != 1 || snap.AverageBatchSize != readers {
		t.Fatalf("bad batching stats: %+v", snap)
	}
}

func TestReadArrivingDuringFenceDoesNotJoinOldRound(t *testing.T) {
	var calls atomic.Int64
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	stats := &readMetrics{}
	coordinator := newReadFenceCoordinator(0, 128, func() error {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		return nil
	}, stats)
	defer coordinator.Close()

	firstResult := make(chan error, 1)
	go func() { firstResult <- coordinator.Wait(context.Background()) }()
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first fence did not start")
	}
	// This read started after the first quorum verification began, so it
	// MUST initiate a different fence (otherwise strong reads may be stale).
	secondResult := make(chan error, 1)
	go func() { secondResult <- coordinator.Wait(context.Background()) }()
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		close(releaseFirst)
		t.Fatal("second read incorrectly joined the already-running fence")
	}
	close(releaseFirst)
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected two fence rounds, got %d", got)
	}
}

func TestReadFenceCancellationAndFailure(t *testing.T) {
	expected := errors.New("no quorum")
	stats := &readMetrics{}
	coordinator := newReadFenceCoordinator(0, 10, func() error { return expected }, stats)
	defer coordinator.Close()
	if err := coordinator.Wait(context.Background()); !errors.Is(err, expected) {
		t.Fatalf("want quorum error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := coordinator.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context canceled, got %v", err)
	}
}
