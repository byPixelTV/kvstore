package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/byPixelTV/kvstore/internal/storage"
)

// BatchWrite and SET share a Raft admission queue but not an atomicity
// boundary. Every acknowledged mutation must have exactly one revision.
func TestBatchRPCAndPointWritesShareOrderedRaftPipeline(t *testing.T) {
	raftAddr, grpcAddr, adminAddr := freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
	cfg := config.Config{Mode: config.ModeCluster, NodeID: "node0", DataDir: t.TempDir(), Bootstrap: true,
		RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr, GRPCAdvertiseAddr: grpcAddr,
		AdminListenAddr: adminAddr, AdminAdvertiseAddr: adminAddr, JoinToken: "test"}
	n, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const singles, batches, batchSize = 32, 8, 8
	revisions := make(chan uint64, singles+batches*batchSize)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < singles; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rev, err := n.Set(ctx, "pipelined", fmt.Sprintf("one-%d", i), []byte("v"))
			if err != nil {
				t.Errorf("point: %v", err)
				return
			}
			revisions <- rev
		}(i)
	}
	for b := 0; b < batches; b++ {
		wg.Add(1)
		go func(b int) {
			defer wg.Done()
			<-start
			ops := make([]storage.WriteOperation, batchSize)
			for i := 0; i < batchSize; i++ {
				ops[i] = storage.WriteOperation{Namespace: "pipelined", Key: fmt.Sprintf("batch-%d-%d", b, i), Value: []byte("v")}
			}
			got, err := n.BatchWrite(ctx, ops)
			if err != nil {
				t.Errorf("batch %d: %v", b, err)
				return
			}
			if len(got) != batchSize {
				t.Errorf("batch response size: %d", len(got))
				return
			}
			for _, rev := range got {
				revisions <- rev
			}
		}(b)
	}
	close(start)
	wg.Wait()
	close(revisions)
	seen := make(map[uint64]bool)
	for rev := range revisions {
		if rev == 0 || seen[rev] {
			t.Errorf("duplicate/zero revision %d", rev)
		}
		seen[rev] = true
	}
	want := uint64(singles + batches*batchSize)
	if uint64(len(seen)) != want {
		t.Fatalf("got %d revisions, want %d", len(seen), want)
	}
	for rev := uint64(1); rev <= want; rev++ {
		if !seen[rev] {
			t.Fatalf("missing revision %d", rev)
		}
	}
	page, err := n.ReadChanges(ctx, 0, int(want+1))
	if err != nil || page.Head != want || uint64(len(page.Events)) != want {
		t.Fatalf("feed head=%d count=%d err=%v", page.Head, len(page.Events), err)
	}
	perf := n.writes.Performance()
	if perf.BatchRPCs != batches || perf.BatchLogicalWrites != batches*batchSize || perf.Submitted != want {
		t.Fatalf("batch metrics not accounted: %+v", perf)
	}
}

func TestMigrationFenceRejectsOnlyTheClientBatch(t *testing.T) {
	raftAddr, grpcAddr, adminAddr := freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
	cfg := config.Config{Mode: config.ModeCluster, NodeID: "node0", DataDir: t.TempDir(), Bootstrap: true,
		RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr, GRPCAdvertiseAddr: grpcAddr,
		AdminListenAddr: adminAddr, AdminAdvertiseAddr: adminAddr, JoinToken: "test"}
	n, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := n.FenceSlot(ctx, "migrate", sharding.Slot("migrate", "sealed"), true); err != nil {
		t.Fatal(err)
	}
	_, err = n.BatchWrite(ctx, []storage.WriteOperation{{Namespace: "migrate", Key: "sealed", Value: []byte("bad")}, {Namespace: "migrate", Key: "other", Value: []byte("bad")}})
	if err == nil {
		t.Fatal("fenced client batch was accepted")
	}
	unrelated := "unrelated"
	for sharding.Slot("migrate", unrelated) == sharding.Slot("migrate", "sealed") {
		unrelated += "-next"
	}
	if _, err := n.Set(ctx, "migrate", unrelated, []byte("ok")); err != nil {
		t.Fatalf("unrelated SET affected: %v", err)
	}
	if perf := n.writes.Performance(); perf.BatchRPCs != 1 {
		t.Fatalf("missing rejected batch statistics: %+v", perf)
	}
}
