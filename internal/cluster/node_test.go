package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	_ = l.Close()
	return address
}

func waitLeader(t *testing.T, n *Node) {
	t.Helper()
	until := time.Now().Add(12 * time.Second)
	for time.Now().Before(until) {
		if n.RaftState() == "Leader" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("node did not elect leader")
}

func TestSingleNodeRaftReopen(t *testing.T) {
	raftAddr := freeTCPAddr(t)
	grpcAddr := freeTCPAddr(t)
	adminAddr := freeTCPAddr(t)
	cfg := config.Config{
		Mode: config.ModeCluster, NodeID: "node0", DataDir: t.TempDir(), Bootstrap: true,
		RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr, GRPCAdvertiseAddr: grpcAddr,
		AdminListenAddr: adminAddr, AdminAdvertiseAddr: adminAddr, JoinToken: "test-only",
	}
	n, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	rev, err := n.Set(ctx, "players", "123", []byte("hello"))
	if err != nil || rev != 1 {
		t.Fatalf("Set rev=%d err=%v", rev, err)
	}
	rec, found, err := n.Get(ctx, "players", "123")
	if err != nil || !found || string(rec.Value) != "hello" {
		t.Fatalf("Get %+v found=%v err=%v", rec, found, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	waitLeader(t, n)
	rec, found, err = n.Get(ctx, "players", "123")
	if err != nil || !found || string(rec.Value) != "hello" || rec.Revision != 1 {
		t.Fatalf("recovered %+v found=%v err=%v", rec, found, err)
	}
	rev, err = n.Delete(ctx, "players", "123")
	if err != nil || rev != 2 {
		t.Fatalf("Delete rev=%d err=%v", rev, err)
	}
}
