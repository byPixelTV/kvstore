//go:build integration

package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestThreeNodeRaftFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	token := "integration-join-token"
	rAddrs := []string{freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)}
	gAddrs := []string{freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)}
	aAddrs := []string{freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)}
	seeds := []string{aAddrs[0], aAddrs[1], aAddrs[2]}
	nodes := make([]*Node, 3)
	servers := make([]*grpc.Server, 3)
	for i := 0; i < 3; i++ {
		cfg := config.Config{
			Mode: config.ModeCluster, NodeID: fmt.Sprintf("node%d", i), DataDir: t.TempDir(),
			Bootstrap: i == 0, RaftBindAddr: rAddrs[i], RaftAdvertiseAddr: rAddrs[i],
			GRPCAdvertiseAddr: gAddrs[i], AdminListenAddr: aAddrs[i], AdminAdvertiseAddr: aAddrs[i],
			JoinToken: token, ClusterSeeds: seeds,
		}
		node, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node
		if err = node.StartAdmin(); err != nil {
			t.Fatal(err)
		}
		lis, err := net.Listen("tcp", gAddrs[i])
		if err != nil {
			t.Fatal(err)
		}
		server := grpc.NewServer()
		servers[i] = server
		kvstorev1.RegisterKVStoreServer(server, grpcserver.New(kvstore.New(node)))
		go server.Serve(lis)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	defer func() {
		for _, server := range servers {
			if server != nil {
				server.Stop()
			}
		}
		for _, n := range nodes {
			if n != nil {
				_ = n.Close()
			}
		}
	}()
	for _, n := range nodes {
		go n.JoinUntilMember(ctx)
		go n.RegisterSelf(ctx)
	}
	waitFor := func(what string, check func() bool) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
			if check() {
				return
			}
			time.Sleep(150 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	waitFor("three voters", func() bool {
		future := nodes[0].raft.GetConfiguration()
		if future.Error() != nil {
			return false
		}
		c := 0
		for _, member := range future.Configuration().Servers {
			if member.Suffrage == raft.Voter {
				c++
			}
		}
		return c == 3
	})
	conn, err := grpc.NewClient(gAddrs[1], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := kvstorev1.NewKVStoreClient(conn)
	res, err := client.Set(ctx, &kvstorev1.SetRequest{Namespace: "players", Key: "123", Value: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRevision() != 1 {
		t.Fatalf("first write revision=%d", res.GetRevision())
	}
	// EVENTUAL is an explicitly opt-in local read from node1's committed FSM.
	// Wait for replication before asserting presence (it may legitimately lag).
	waitFor("local follower replication", func() bool {
		reply, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: "players", Key: "123", Consistency: kvstorev1.ReadConsistency_READ_CONSISTENCY_EVENTUAL})
		return err == nil && reply.GetFound() && string(reply.GetValue()) == "hello"
	})
	strongReply, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: "players", Key: "123"})
	if err != nil || !strongReply.GetFound() {
		t.Fatalf("strong read reply=%v err=%v", strongReply, err)
	}

	head, err := client.Head(ctx, &kvstorev1.HeadRequest{})
	if err != nil || head.GetRevision() != 1 {
		t.Fatalf("head=%+v err=%v", head, err)
	}
	scan, err := client.Scan(ctx, &kvstorev1.ScanRequest{Namespace: "players"})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := scan.Recv()
	if err != nil || !checkpoint.GetCheckpoint() || checkpoint.GetSnapshotRevision() != 1 {
		t.Fatalf("scan checkpoint=%+v err=%v", checkpoint, err)
	}
	item, err := scan.Recv()
	if err != nil || item.GetKey() != "123" {
		t.Fatalf("scan item=%+v err=%v", item, err)
	}
	watch, err := client.Watch(ctx, &kvstorev1.WatchRequest{Namespace: "players", AfterRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := watch.Recv()
	if err != nil || !initial.GetCheckpoint() {
		t.Fatalf("watch checkpoint=%+v err=%v", initial, err)
	}
	// Stop the original leader and expect another quorum leader to emerge.
	servers[0].Stop()
	if err := nodes[0].Close(); err != nil {
		t.Fatal(err)
	}
	nodes[0] = nil
	waitFor("new leader", func() bool { return nodes[1].RaftState() == "Leader" || nodes[2].RaftState() == "Leader" })
	got, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: "players", Key: "123"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.GetFound() || string(got.GetValue()) != "hello" {
		t.Fatalf("value missing after failover: %+v", got)
	}
	// A stream established before failover must continue receiving replicated
	// events from the surviving node, even when it is not the new leader.
	if _, err := client.Set(ctx, &kvstorev1.SetRequest{Namespace: "players", Key: "456", Value: []byte("after-failover")}); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	change, err := watch.Recv()
	if err != nil || change.GetKey() != "456" || change.GetRevision() != 2 {
		t.Fatalf("watch after failover=%+v err=%v", change, err)
	}

	// A forwarded BatchWrite uses one Raft command with ordered revisions,
	// including deletes of previously missing keys.
	batch, err := client.BatchWrite(ctx, &kvstorev1.BatchWriteRequest{Operations: []*kvstorev1.BatchWriteOperation{
		{Namespace: "players", Key: "bulk", Value: []byte("one")},
		{Namespace: "players", Key: "bulk", Value: []byte("two")},
		{Namespace: "players", Key: "bulk", Delete: true},
		{Namespace: "players", Key: "bulk", Delete: true},
	}})
	if err != nil {
		t.Fatalf("forwarded BatchWrite: %v", err)
	}
	for i, want := range []uint64{3, 4, 5, 0} {
		if len(batch.GetResults()) != 4 || batch.Results[i].GetRevision() != want {
			t.Fatalf("batch result %d: %+v", i, batch.GetResults())
		}
	}
}
