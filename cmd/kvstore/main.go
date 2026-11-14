package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/cluster"
	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/healthcheck"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	"github.com/byPixelTV/kvstore/internal/storage"
	pebblestore "github.com/byPixelTV/kvstore/internal/storage/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var version = "dev" // -ldflags=-X main.version=...

func main() {
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case "--version", "-version":
			fmt.Println("kvstore", version)
			return
		case "healthcheck":
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			addr := os.Getenv("KVSTORE_HEALTH_ADDR")
			if addr == "" {
				addr = "127.0.0.1:50051"
			}
			if err := healthcheck.Check(ctx, addr); err != nil {
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
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	var store storage.Store
	var node *cluster.Node
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
	} else {
		persistent, openErr := pebblestore.OpenWithOptions(cfg.DataDir, pebblestore.Options{BatchWindow: cfg.WriteBatchWindow, BatchMax: cfg.WriteBatchMax})
		if openErr != nil {
			return fmt.Errorf("open pebble: %w", openErr)
		}
		defer func() { runErr = errors.Join(runErr, persistent.Close()) }()
		store = persistent
	}

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %s: %w", cfg.ListenAddr, err)
	}
	defer listener.Close()
	service := kvstore.New(store)
	grpcServer := grpc.NewServer()
	kvstorev1.RegisterKVStoreServer(grpcServer, grpcserver.New(service))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("liveness", healthpb.HealthCheckResponse_SERVING)
	if node == nil {
		healthSrv.SetServingStatus("readiness", healthpb.HealthCheckResponse_SERVING)
	} else {
		healthSrv.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if node != nil {
		go node.RegisterSelf(ctx)
		go node.JoinUntilMember(ctx)
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				state := healthpb.HealthCheckResponse_NOT_SERVING
				if node.Ready() {
					state = healthpb.HealthCheckResponse_SERVING
				}
				healthSrv.SetServingStatus("readiness", state)
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
