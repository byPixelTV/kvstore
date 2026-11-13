package healthcheck

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestCheckHealth(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	server := grpc.NewServer()
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	healthServer.SetServingStatus("readiness", healthpb.HealthCheckResponse_SERVING)
	if err := Check(ctx, listener.Addr().String()); err != nil {
		t.Fatalf("expected SERVING: %v", err)
	}

	healthServer.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
	if err := Check(ctx, listener.Addr().String()); err == nil {
		t.Fatal("expected NOT_SERVING to fail")
	}
}
