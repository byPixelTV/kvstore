package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
)

// Many writes must remain unique and ordered in Raft even though futures are
// awaited concurrently. This also catches broken result-to-request mapping.
func TestPipelinedWritesPreserveRevisionsAndFeed(t *testing.T) {
	raftAddr, grpcAddr, adminAddr := freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
	cfg := config.Config{
		Mode: config.ModeCluster, NodeID: "node0", DataDir: t.TempDir(), Bootstrap: true,
		RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr,
		GRPCAdvertiseAddr: grpcAddr, AdminListenAddr: adminAddr, AdminAdvertiseAddr: adminAddr,
		JoinToken: "test-only", WriteBatchWindow: 50 * time.Microsecond,
		WriteBatchMax: 16, RaftMaxInflight: 8,
	}
	node, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	waitLeader(t, node)

	const total = 512
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, total)
	revisions := make(chan uint64, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rev, err := node.Set(ctx, "load", fmt.Sprintf("key-%04d", i), []byte("value"))
			if err != nil {
				errs <- err
				return
			}
			revisions <- rev
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("write: %v", err)
	}
	if t.Failed() {
		return
	}
	close(revisions)

	seen := make(map[uint64]bool, total)
	for rev := range revisions {
		if rev == 0 || seen[rev] {
			t.Errorf("duplicate/invalid revision %d", rev)
		}
		seen[rev] = true
	}
	if len(seen) != total {
		t.Fatalf("got %d revisions, want %d", len(seen), total)
	}
	page, err := node.ReadChanges(ctx, 0, total+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != total || page.Head != total {
		t.Fatalf("head=%d events=%d, want %d", page.Head, len(page.Events), total)
	}
	for i, event := range page.Events {
		if event.Revision != uint64(i+1) {
			t.Fatalf("event %d revision %d", i, event.Revision)
		}
	}
	perf := node.writes.Performance()
	if perf.Submitted != total || perf.Groups < 2 || perf.Completed != perf.Groups || perf.GroupErrors != 0 {
		t.Fatalf("unexpected write performance %+v", perf)
	}
	if perf.InFlight != 0 || perf.PeakInFlight > int64(cfg.RaftMaxInflight) {
		t.Fatalf("unbounded inflight writes: %+v", perf)
	}
}
