package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/byPixelTV/kvstore/internal/monitoring"
	pebblestore "github.com/byPixelTV/kvstore/internal/storage/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func startStandaloneMetrics(store *pebblestore.Store, path string) (grpc.UnaryServerInterceptor, func(), error) {
	token := os.Getenv("KVSTORE_METRICS_TOKEN")
	if token == "" {
		return nil, func() {}, nil
	}
	addr := os.Getenv("KVSTORE_STANDALONE_ADMIN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:7002"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen for standalone metrics: %w", err)
	}
	registry := prometheus.NewRegistry()
	observe := monitoring.RegisterRPC(registry)
	backend := prometheus.NewRegistry()
	backend.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "kvstore_pebble_failed_compactions_total", Help: "Pebble compactions that failed.",
	}, func() float64 {
		_, failed := store.CompactionCounts()
		return float64(failed)
	}))
	mux := http.NewServeMux()
	metrics := promhttp.HandlerFor(prometheus.Gatherers{registry, backend}, promhttp.HandlerOpts{})
	mux.HandleFunc("GET /metrics", monitoring.Protect(token, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Use a scrape-local registry for disk stats so an unsupported filesystem
		// omits the gauges instead of triggering a false disk-space alert.
		available, total, err := monitoring.DiskBytes(path)
		if err != nil {
			metrics.ServeHTTP(w, r)
			return
		}
		disk := prometheus.NewRegistry()
		disk.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "kvstore_disk_available_bytes", Help: "Bytes available on the data filesystem.",
		}, func() float64 { return float64(available) }))
		disk.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "kvstore_disk_total_bytes", Help: "Total bytes on the data filesystem.",
		}, func() float64 { return float64(total) }))
		promhttp.HandlerFor(prometheus.Gatherers{registry, backend, disk}, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	})))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("standalone metrics listener exited", "error", err)
		}
	}()
	return observe, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			_ = server.Close()
		}
		_ = listener.Close()
	}, nil
}
