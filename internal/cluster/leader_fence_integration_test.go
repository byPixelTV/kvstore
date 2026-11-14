package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
)

func TestStrongReadFenceAmortizesBarrierOnLeader(t *testing.T) {
	raftAddr, grpcAddr, adminAddr := freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
	cfg := config.Config{
		Mode: config.ModeCluster, NodeID: "node0", DataDir: t.TempDir(), Bootstrap: true,
		RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr,
		GRPCAdvertiseAddr: grpcAddr, AdminListenAddr: adminAddr, AdminAdvertiseAddr: adminAddr,
		JoinToken: "test-only", ReadBatchWindow: time.Microsecond, ReadBatchMax: 256,
		WriteBatchWindow: 50 * time.Microsecond, WriteBatchMax: 16, RaftMaxInflight: 4,
	}
	node, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	waitLeader(t, node)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := node.Set(ctx, "test", "key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		rec, found, err := node.Get(ctx, "test", "key")
		if err != nil {
			t.Fatal(err)
		}
		if !found || string(rec.Value) != "value" {
			t.Fatalf("stale read: %+v found=%t", rec, found)
		}
	}
	stats := node.ReadPerformance()
	if stats.BarrierCalls != 1 {
		t.Fatalf("expected one barrier per leader term, got %+v", stats)
	}
	if stats.VerifyLeaderCalls != 32 {
		t.Fatalf("expected quorum check per read batch, got %+v", stats)
	}
}
