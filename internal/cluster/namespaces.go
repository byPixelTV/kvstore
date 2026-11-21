package cluster

import (
	"context"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (n *Node) ListNamespaces(ctx context.Context) ([]string, error) {
	if !n.ClientTrafficReady() {
		return nil, status.Error(codes.Unavailable, "Raft voter configuration not ready")
	}
	if n.raft.State() != raft.Leader {
		leader, err := n.leaderPeer()
		if err != nil {
			return nil, err
		}
		if leader.ID == n.cfg.NodeID {
			return nil, status.Error(codes.Unavailable, "leader transition")
		}
		conn, err := n.peerConn(leader.GRPCAddr)
		if err != nil {
			return nil, err
		}
		result, err := kvstorev1.NewConsoleClient(conn).ListNamespaces(ctx, &kvstorev1.ListNamespacesRequest{})
		return result.GetNamespaces(), err
	}
	if err := n.readStrong(ctx); err != nil {
		return nil, err
	}
	return n.fsm.ListNamespaces(ctx)
}
