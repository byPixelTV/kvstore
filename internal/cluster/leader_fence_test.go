package cluster

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLeaderFenceInitialAndPerTermBarrier(t *testing.T) {
	var term atomic.Uint64
	term.Store(4)
	var barriers, verified atomic.Int64
	g := &leaderFence{
		term:    func() (uint64, error) { return term.Load(), nil },
		barrier: func() error { barriers.Add(1); return nil },
		verify:  func() error { verified.Add(1); return nil },
	}
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- g.Fence() }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := barriers.Load(); got != 1 {
		t.Fatalf("got %d initial barriers, want 1", got)
	}
	if got := verified.Load(); got != 64 {
		t.Fatalf("got %d verifications, want 64", got)
	}
	term.Store(5)
	if err := g.Fence(); err != nil {
		t.Fatal(err)
	}
	if got := barriers.Load(); got != 2 {
		t.Fatalf("term change needs new barrier, got %d", got)
	}
}

func TestLeaderFenceFailuresFailClosed(t *testing.T) {
	var term atomic.Uint64
	term.Store(8)
	var barriers atomic.Int64
	failQuorum := errors.New("no quorum")
	g := &leaderFence{
		term:    func() (uint64, error) { return term.Load(), nil },
		barrier: func() error { barriers.Add(1); return nil },
		verify:  func() error { return failQuorum },
	}
	if err := g.Fence(); !errors.Is(err, failQuorum) {
		t.Fatalf("want quorum error, got %v", err)
	}
	if barriers.Load() != 1 {
		t.Fatal("initial barrier missing")
	}

	g.verify = func() error { term.Store(9); return nil }
	if err := g.Fence(); err == nil {
		t.Fatal("term change during verify must fail")
	}
	g.verify = func() error { return nil }
	if err := g.Fence(); err != nil {
		t.Fatal(err)
	}
	if barriers.Load() != 2 {
		t.Fatalf("term 9 requires fresh barrier, got %d", barriers.Load())
	}
}

func TestLeaderFenceBarrierFailureNeverCached(t *testing.T) {
	var barriers atomic.Int64
	g := &leaderFence{term: func() (uint64, error) { return 2, nil }, barrier: func() error {
		if barriers.Add(1) == 1 {
			return errors.New("disk failure")
		}
		return nil
	}, verify: func() error { return nil }}
	if err := g.Fence(); err == nil {
		t.Fatal("expected barrier failure")
	}
	if err := g.Fence(); err != nil {
		t.Fatal(err)
	}
	if barriers.Load() != 2 {
		t.Fatalf("need retry: %d", barriers.Load())
	}
}
