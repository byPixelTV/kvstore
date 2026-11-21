package healthcheck

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Check probes the standard gRPC health service on addr.
// The caller must supply a bounded context (usually 2 seconds).
func Check(ctx context.Context, addr string) error {
	return CheckService(ctx, addr, "readiness")
}

func CheckService(ctx context.Context, addr, service string) error {
	if service != "readiness" && service != "liveness" {
		return fmt.Errorf("unknown health service %q", service)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer conn.Close()

	response, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		return fmt.Errorf("check %s at %s: %w", service, addr, err)
	}
	if response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("%s status at %s is %s", service, addr, response.GetStatus())
	}
	return nil
}

// ListenLocal exposes only the health service for probes of an mTLS listener.
func ListenLocal(addr string, health healthpb.HealthServer) (func(), error) {
	if addr == "" {
		return func() {}, nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return nil, fmt.Errorf("KVSTORE_HEALTH_LISTEN_ADDR must use a loopback IP and port")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for local health checks: %w", err)
	}
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, health)
	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			slog.Error("local health listener exited", "error", err)
		}
	}()
	return func() {
		server.Stop()
		_ = listener.Close()
	}, nil
}
