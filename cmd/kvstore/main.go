package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/access"
	"github.com/byPixelTV/kvstore/internal/cluster"
	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/healthcheck"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/byPixelTV/kvstore/internal/storage"
	pebblestore "github.com/byPixelTV/kvstore/internal/storage/pebble"
	"github.com/byPixelTV/kvstore/internal/transaction"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var version = "dev" // -ldflags=-X main.version=...

func main() {
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "--version", "-version":
			fmt.Println("kvstore", version)
			return
		case "apikey":
			if err := runAPIKey(os.Args[2:], os.Stdout, os.Stderr); err != nil {
				slog.Error("API key command failed", "error", err)
				os.Exit(1)
			}
			return
		case "healthcheck":
			if len(os.Args) > 3 {
				slog.Error("usage: kvstore healthcheck [readiness|liveness]")
				os.Exit(1)
			}
			service := "readiness"
			if len(os.Args) == 3 {
				service = os.Args[2]
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			addr := os.Getenv("KVSTORE_HEALTH_ADDR")
			if addr == "" {
				addr = "127.0.0.1:50051"
			}
			if err := healthcheck.CheckService(ctx, addr, service); err != nil {
				slog.Error("healthcheck failed", "error", err)
				os.Exit(1)
			}
			return
		}
	}
	if err := run(); err != nil {
		slog.Error("kvstore exited", "error", err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	switch os.Getenv("KVSTORE_MODE") {
	case "router":
		return sharding.ServeRouter()
	case config.ModeCluster:
		if runtime.GOOS == "windows" {
			return errors.New("Raft voters are unsupported on native Windows. Use KVSTORE_MODE=standalone or run the cluster in Linux containers")
		}
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	endpoint, err := access.Load()
	if err != nil {
		return err
	}
	if cfg.Mode == config.ModeCluster && endpoint.UsesClientAuthentication() {
		return errors.New("API keys and native gRPC TLS require standalone or router mode; use a private encrypted network for Raft voters")
	}
	var store storage.Store
	var transactional transaction.Backend
	var node *cluster.Node
	var listNamespaces func(context.Context) ([]string, error)
	grpcOptions := endpoint.ServerOptions()
	if cfg.Mode == config.ModeCluster {
		node, err = cluster.Open(cfg)
		if err != nil {
			return fmt.Errorf("start raft node: %w", err)
		}
		defer func() { runErr = errors.Join(runErr, node.Close()) }()
		if err = node.StartAdmin(); err != nil {
			return fmt.Errorf("start cluster admin listener: %w", err)
		}
		store = node
		transactional = node
		listNamespaces = node.ListNamespaces
	} else {
		persistent, openErr := pebblestore.OpenWithOptions(cfg.DataDir, pebblestore.Options{BatchWindow: cfg.WriteBatchWindow, BatchMax: cfg.WriteBatchMax})
		if openErr != nil {
			return fmt.Errorf("open pebble: %w", openErr)
		}
		defer func() { runErr = errors.Join(runErr, persistent.Close()) }()
		store = persistent
		transactional = persistent
		listNamespaces = persistent.ListNamespaces
		observe, stopMetrics, err := startStandaloneMetrics(persistent, cfg.DataDir)
		if err != nil {
			return err
		}
		defer stopMetrics()
		if observe != nil {
			grpcOptions = append(grpcOptions, grpc.UnaryInterceptor(observe))
		}
	}

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %s: %w", cfg.ListenAddr, err)
	}
	defer listener.Close()
	service := kvstore.New(store)
	if node != nil {
		grpcOptions = append(grpcOptions, grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if strings.HasPrefix(info.FullMethod, "/kvstore.v1.KVStore/") && !node.ClientTrafficReady() {
				return status.Error(codes.Unavailable, "Raft voter configuration not ready")
			}
			return handler(srv, stream)
		}))
		grpcOptions = append(grpcOptions, grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if strings.HasPrefix(info.FullMethod, "/kvstore.v1.KVStore/") && !node.ClientTrafficReady() {
				return nil, status.Error(codes.Unavailable, "Raft voter configuration not ready")
			}
			// Replicated fences are privileged. Other public RPCs cannot toggle
			// the write protection used by online migration.
			if info.FullMethod == kvstorev1.KVStore_FenceSlot_FullMethodName ||
				info.FullMethod == kvstorev1.KVStore_ControlTransaction_FullMethodName {
				md, ok := metadata.FromIncomingContext(ctx)
				if !ok || cfg.JoinToken == "" {
					return nil, status.Error(codes.PermissionDenied, "internal operation requires authorization")
				}
				auth := md.Get("authorization")
				if len(auth) != 1 || subtle.ConstantTimeCompare([]byte(strings.TrimSpace(strings.TrimPrefix(auth[0], "Bearer "))), []byte(cfg.JoinToken)) != 1 || !strings.HasPrefix(auth[0], "Bearer ") {
					return nil, status.Error(codes.PermissionDenied, "internal operation requires authorization")
				}
			}
			// Topology is revealed only when a client explicitly opts into
			// leader discovery, not on every public unary RPC.
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				request := md.Get("kvstore-discover-leader")
				if len(request) == 1 && request[0] == "1" {
					if addr := node.LeaderGRPCAddr(); addr != "" {
						_ = grpc.SetHeader(ctx, metadata.Pairs("kvstore-leader-grpc", addr))
					}
				}
			}
			return handler(ctx, req)
		}))
	}
	grpcServer := grpc.NewServer(grpcOptions...)
	endpoint.RegisterConsole(grpcServer, listNamespaces)
	if node != nil {
		kvstorev1.RegisterKVStoreServer(grpcServer, grpcserver.New(service, node).WithTransactions(transactional, true))
	} else {
		kvstorev1.RegisterKVStoreServer(grpcServer, grpcserver.New(service).WithTransactions(transactional, false))
	}
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	stopLocalHealth, err := healthcheck.ListenLocal(endpoint.HealthListenAddr, healthSrv)
	if err != nil {
		return err
	}
	defer stopLocalHealth()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("liveness", healthpb.HealthCheckResponse_SERVING)
	if node == nil {
		healthSrv.SetServingStatus("readiness", healthpb.HealthCheckResponse_SERVING)
	} else {
		healthSrv.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
		healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	endpoint.WatchAPIKeys(ctx)
	if node != nil {
		go node.RegisterSelf(ctx)
		go node.JoinUntilMember(ctx)
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				node.RefreshAdmission()
				state := healthpb.HealthCheckResponse_NOT_SERVING
				if node.Ready() {
					state = healthpb.HealthCheckResponse_SERVING
				}
				healthSrv.SetServingStatus("readiness", state)
				healthSrv.SetServingStatus("", state)
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}

	serveErrors := make(chan error, 1)
	go func() { serveErrors <- grpcServer.Serve(listener) }()
	slog.Info("kvstore started", "mode", cfg.Mode, "node_id", cfg.NodeID, "grpc", cfg.ListenAddr, "version", version)
	select {
	case err := <-serveErrors:
		healthSrv.Shutdown()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return err
		}
		return nil
	case <-ctx.Done():
		slog.Info("shutting down kvstore")
		healthSrv.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
		finished := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(finished) }()
		timer := time.NewTimer(cfg.ShutdownTimeout)
		defer timer.Stop()
		select {
		case <-finished:
		case <-timer.C:
			grpcServer.Stop()
			<-finished
		}
		<-serveErrors
		healthSrv.Shutdown()
		return nil
	}
}
