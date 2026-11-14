package main

import (
	"sync"
	"testing"
	"time"
)

func TestPacerSharedAndWeightedReservations(t *testing.T) {
	p := newLogicalPacer(1000)
	deadline := time.Now().Add(time.Second)
	if !p.Wait(10, deadline) {
		t.Fatal("first reservation rejected")
	}
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !p.Wait(10, deadline) {
				t.Error("unexpected rejection")
			}
		}()
	}
	wg.Wait()
	if time.Since(start) < 10*time.Millisecond {
		t.Fatal("batch reservations ignored logical operation weight")
	}
	if newLogicalPacer(0) != nil {
		t.Fatal("unlimited mode should have no pacer")
	}
	if p.Wait(1, time.Now().Add(-time.Second)) {
		t.Fatal("accepted reservation past deadline")
	}
}
