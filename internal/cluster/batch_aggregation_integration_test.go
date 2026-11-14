package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/byPixelTV/kvstore/internal/storage"
)

// This test sends many small BatchWrite RPCs at once. Prior to physical
// aggregation, each RPC always generated one Raft log. Now several RPCs can
// share a log while retaining per-client results and revision order.
func TestConcurrentClientBatchesAggregateInRaft(t *testing.T) {
	raftAddr, grpcAddr, adminAddr := freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
	cfg := config.Config{Mode: config.ModeCluster, NodeID: "node0", DataDir: t.TempDir(), Bootstrap: true,
		RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr,
		GRPCAdvertiseAddr: grpcAddr, AdminListenAddr: adminAddr, AdminAdvertiseAddr: adminAddr,
		JoinToken: "test", WriteBatchWindow: 2 * time.Millisecond, WriteBatchMaxWindow: 5 * time.Millisecond,
		WriteBatchTarget: 64, WriteBatchMax: 256, RaftMaxInflight: 4}
	n, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	const clients, size = 32, 8
	var wg sync.WaitGroup
	revisions := make(chan uint64, clients*size)
	start := make(chan struct{})
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			operations := make([]storage.WriteOperation, size)
			for j := range operations {
				operations[j] = storage.WriteOperation{Namespace: "agg", Key: fmt.Sprintf("c%03d-k%d", i, j), Value: []byte("value")}
			}
			got, err := n.BatchWrite(ctx, operations)
			if err != nil {
				t.Errorf("batch %d: %v", i, err)
				return
			}
			if len(got) != size {
				t.Errorf("batch %d revision count %d", i, len(got))
				return
			}
			for _, revision := range got {
				revisions <- revision
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(revisions)
	if t.Failed() {
		return
	}
	seen := make(map[uint64]bool)
	for revision := range revisions {
		if revision == 0 || seen[revision] {
			t.Fatalf("invalid/duplicate revision %d", revision)
		}
		seen[revision] = true
	}
	if len(seen) != clients*size {
		t.Fatalf("got %d revisions", len(seen))
	}
	page, err := n.ReadChanges(ctx, 0, clients*size+1)
	if err != nil || len(page.Events) != clients*size {
		t.Fatalf("missing changefeed events: %d %v", len(page.Events), err)
	}
	perf := n.writes.Performance()
	// Each client is its own RPC. The physical number of Raft logs should
	// normally be smaller under 32 concurrent writers and a 2ms window.
	if perf.BatchRPCs != clients || perf.Submitted != clients*size || perf.Groups >= clients || perf.Completed != perf.Groups {
		t.Fatalf("aggregation not exercised: %+v", perf)
	}
}
