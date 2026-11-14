package pebblestore

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// Local storage microbenchmarks for separating disk cost from gRPC/Raft cost.
// The end-to-end cmd/kvbench measures are more relevant for SDK performance.
func BenchmarkPebbleGet(b *testing.B) {
	s, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		if _, err := s.Set(ctx, "bench", fmt.Sprintf("k%d", i), make([]byte, 256)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, found, err := s.Get(ctx, "bench", fmt.Sprintf("k%d", i%1000))
		if err != nil || !found {
			b.Fatalf("get: %v", err)
		}
	}
}
func BenchmarkPebbleSetSync(b *testing.B) {
	s, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	value := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Set(ctx, "bench", fmt.Sprintf("k%d", i%1000), value); err != nil {
			b.Fatal(err)
		}
	}
}

// Measures concurrent sync writes with the same real record+revision+feed
// contract as production, rather than raw Pebble batch throughput.
func BenchmarkPebbleSetParallelGrouped(b *testing.B) {
	s, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	var next atomic.Uint64
	value := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := next.Add(1)
			if _, err := s.Set(context.Background(), "bench", fmt.Sprintf("k%d", id%1000), value); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
