package main

import (
	"context"
	"net"
	"testing"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type leaderProbeServer struct {
	kvstorev1.UnimplementedKVStoreServer
}

func (leaderProbeServer) Head(ctx context.Context, _ *kvstorev1.HeadRequest) (*kvstorev1.HeadResponse, error) {
	// Emulate the opt-in metadata contract used by the real server.
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get("kvstore-discover-leader")) > 0 {
		if err := grpc.SetHeader(ctx, metadata.Pairs("kvstore-leader-grpc", "node1:50051")); err != nil {
			return nil, err
		}
	}
	return &kvstorev1.HeadResponse{}, nil
}
func TestResolveLeaderMappedAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	kvstorev1.RegisterKVStoreServer(srv, leaderProbeServer{})
	go srv.Serve(ln)
	defer srv.Stop()
	defer ln.Close()
	mapped, err := resolveLeaderAddress(ln.Addr().String(), "node0:50051=127.0.0.1:50061,node1:50051=127.0.0.1:50062", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if mapped != "127.0.0.1:50062" {
		t.Fatalf("wrong leader: %q", mapped)
	}
	if _, err = resolveLeaderAddress(ln.Addr().String(), "node0:50051=127.0.0.1:50061", 3*time.Second); err == nil {
		t.Fatal("unrecognized leader must be rejected")
	}
}
