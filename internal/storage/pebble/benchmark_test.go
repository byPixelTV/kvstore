package pebblestore

import (
	"context"
	"fmt"
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
