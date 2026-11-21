package sharding

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	api "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type namespacePeer struct {
	api.UnimplementedConsoleServer
	names []string
}

func (p namespacePeer) ListNamespaces(context.Context, *api.ListNamespacesRequest) (*api.ListNamespacesResponse, error) {
	return &api.ListNamespacesResponse{Namespaces: p.names}, nil
}

func TestNamespaceDiscoveryAcrossShards(t *testing.T) {
	router := &Router{}
	for shard, names := range [][]string{{"players", "__kvstore_internal_routing"}, {"games", "players"}, {"audit"}} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := grpc.NewServer()
		api.RegisterConsoleServer(server, namespacePeer{names: names})
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		router.groups[shard] = &group{conns: [3][]*grpc.ClientConn{{conn}}}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	want := []string{"audit", "games", "players"}
	if names, err := router.ListNamespaces(ctx); err != nil || !slices.Equal(names, want) {
		t.Fatalf("namespace discovery got %v, %v; want %v", names, err, want)
	}
	router.groups[2].leader.Store(-1)
	if names, err := router.ListNamespaces(ctx); status.Code(err) != codes.Unavailable || len(names) != 0 {
		t.Fatalf("discovery returned a partial catalog: %v, %v", names, err)
	}
}
