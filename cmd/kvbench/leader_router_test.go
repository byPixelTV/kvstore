package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type routingFakeServer struct {
	kvstorev1.UnimplementedKVStoreServer
	advertised   *atomic.Value
	value        string
	writes       atomic.Uint64
	rejectWrites bool
}

func (s *routingFakeServer) Head(ctx context.Context, _ *kvstorev1.HeadRequest) (*kvstorev1.HeadResponse, error) {
	if s.advertised != nil {
		if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get("kvstore-discover-leader")) == 1 {
			_ = grpc.SetHeader(ctx, metadata.Pairs("kvstore-leader-grpc", s.advertised.Load().(string)))
		}
	}
	return &kvstorev1.HeadResponse{}, nil
}
func (s *routingFakeServer) Get(context.Context, *kvstorev1.GetRequest) (*kvstorev1.GetResponse, error) {
	return &kvstorev1.GetResponse{Found: true, Value: []byte(s.value)}, nil
}
func (s *routingFakeServer) Set(context.Context, *kvstorev1.SetRequest) (*kvstorev1.SetResponse, error) {
	s.writes.Add(1)
	if s.rejectWrites {
		return nil, status.Error(codes.Unavailable, "simulated uncertain write")
	}
	return &kvstorev1.SetResponse{Revision: 1}, nil
}

func startRoutingFake(t *testing.T, fake *routingFakeServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	kvstorev1.RegisterKVStoreServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener.Addr().String()
}

func TestLeaderRouterSwitchesToDiscoveredLeader(t *testing.T) {
	var advertised atomic.Value
	advertised.Store("node1:50051")
	bootstrap := startRoutingFake(t, &routingFakeServer{advertised: &advertised, value: "fallback"})
	one := startRoutingFake(t, &routingFakeServer{value: "one"})
	two := startRoutingFake(t, &routingFakeServer{value: "two"})
	pool, err := newLeaderRoutePool(bootstrap, "node1:50051="+one+",node2:50051="+two, 2, 3*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client := pool.Clients()[1]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, &kvstorev1.GetRequest{})
	if err != nil || string(resp.GetValue()) != "one" {
		t.Fatalf("wrong initial route: resp=%v err=%v", resp, err)
	}
	advertised.Store("node2:50051")
	if err := pool.Refresh(); err != nil {
		t.Fatal(err)
	}
	resp, err = client.Get(ctx, &kvstorev1.GetRequest{})
	if err != nil || string(resp.GetValue()) != "two" {
		t.Fatalf("wrong new route: resp=%v err=%v", resp, err)
	}
	if pool.switches.Load() < 2 {
		t.Fatalf("expected route changes, got %d", pool.switches.Load())
	}
}

func TestLeaderRouterDoesNotBlindlyRetryUncertainWrite(t *testing.T) {
	var advertised atomic.Value
	advertised.Store("node1:50051")
	fallbackServer := &routingFakeServer{advertised: &advertised, value: "fallback"}
	bootstrap := startRoutingFake(t, fallbackServer)
	leader := &routingFakeServer{value: "direct", rejectWrites: true}
	one := startRoutingFake(t, leader)
	pool, err := newLeaderRoutePool(bootstrap, "node1:50051="+one, 1, 3*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client := pool.Clients()[0]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = client.Set(ctx, &kvstorev1.SetRequest{Namespace: "x", Key: "key", Value: []byte("v")})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("wanted uncertain unavailable write, got %v", err)
	}
	if leader.writes.Load() != 1 || fallbackServer.writes.Load() != 0 {
		t.Fatalf("write was retried: leader=%d fallback=%d", leader.writes.Load(), fallbackServer.writes.Load())
	}
	if pool.failovers.Load() != 1 {
		t.Fatalf("expected one failover, got %d", pool.failovers.Load())
	}
	resp, err := client.Get(ctx, &kvstorev1.GetRequest{})
	if err != nil || string(resp.GetValue()) != "fallback" {
		t.Fatalf("fallback read failed: %v, %v", resp, err)
	}
}

func TestLeaderMapRejectsNonAllowlistedEndpoints(t *testing.T) {
	if err := validateLeaderMap("node1:50051"); err == nil {
		t.Fatal("malformed map accepted")
	}
	if err := validateLeaderMap("node1:50051=not-a-hostport"); err == nil {
		t.Fatal("bad address accepted")
	}
}
