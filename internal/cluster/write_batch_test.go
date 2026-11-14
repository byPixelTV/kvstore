package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
)

func TestLeaderWriteMicrobatchPreservesAllRevisionsAndFeed(t *testing.T) {
	raftAddr, grpcAddr, adminAddr := freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
	cfg := config.Config{
		Mode: config.ModeCluster, NodeID: "node0", DataDir: t.TempDir(), Bootstrap: true,
		RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr,
		GRPCAdvertiseAddr: grpcAddr, AdminListenAddr: adminAddr, AdminAdvertiseAddr: adminAddr,
		JoinToken: "test-only",
	}
	node, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	waitLeader(t, node)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const count = 64
	var wg sync.WaitGroup
	start := make(chan struct{})
	revisions := make(chan uint64, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rev, err := node.Set(ctx, "load", fmt.Sprintf("key-%d", i), []byte("value"))
			if err != nil {
				t.Errorf("set %d: %v", i, err)
				return
			}
			revisions <- rev
		}(i)
	}
	close(start)
	wg.Wait()
	close(revisions)
	seen := make(map[uint64]bool, count)
	for rev := range revisions {
		if rev == 0 || seen[rev] {
			t.Errorf("duplicate/zero rev %d", rev)
		}
		seen[rev] = true
	}
	if len(seen) != count {
		t.Fatalf("got %d writes, want %d", len(seen), count)
	}
	for rev := uint64(1); rev <= count; rev++ {
		if !seen[rev] {
			t.Fatalf("missing rev %d", rev)
		}
	}
	page, err := node.ReadChanges(ctx, 0, count+1)
	if err != nil || len(page.Events) != count || page.Head != count {
		t.Fatalf("feed head=%d events=%d err=%v", page.Head, len(page.Events), err)
	}
	for i, event := range page.Events {
		if event.Revision != uint64(i+1) {
			t.Fatalf("event %d has rev %d", i, event.Revision)
		}
	}
}
