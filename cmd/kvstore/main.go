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
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	pebblestore "github.com/byPixelTV/kvstore/internal/storage/pebble"
	"google.golang.org/grpc"
)

// Overridden by -ldflags "-X main.version=..." in CI and Docker.
var version = "dev"

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-version") {
		fmt.Println("kvstore", version)
		return
	}

	if err := run(); err != nil {
		slog.Error("kvstore stopped with error", "error", err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	address := os.Getenv("KVSTORE_LISTEN_ADDR")
	if address == "" {
		address = "127.0.0.1:50051"
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			slog.Error("failed to close listener", "error", err)
		}
	}(listener)

	dataDir := os.Getenv("KVSTORE_DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}
	store, err := pebblestore.Open(dataDir)
	if err != nil {
		return fmt.Errorf("open persistent storage: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			slog.Error("failed to close storage", "error", err)
			runErr = errors.Join(runErr, fmt.Errorf("close storage: %w", err))
		}
	}()

	service := kvstore.New(store)
	grpcServer := grpc.NewServer()
	kvstorev1.RegisterKVStoreServer(grpcServer, grpcserver.New(service))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- grpcServer.Serve(listener)
	}()

	slog.Info("kvstore started", "version", version, "address", address, "data_dir", dataDir)

	select {
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve grpc: %w", err)
		}
		return nil
	case <-ctx.Done():
		slog.Info("shutting down kvstore")
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()

		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-stopped:
		case <-timer.C:
			slog.Warn("shutdown deadline exceeded, forcing stop")
			grpcServer.Stop()
			<-stopped
		}
		return nil
	}
}
