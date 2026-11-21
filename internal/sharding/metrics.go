package sharding

import (
	"context"
	"net/http"
	"strconv"
	"time"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (r *Router) metrics() (http.Handler, grpc.UnaryServerInterceptor) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kvstore_router_ready", Help: "Whether the router has a usable route for every shard.",
	}, func() float64 { return gauge(r.Ready()) }))
	for i, group := range r.groups {
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "kvstore_shard_ready", Help: "Whether the router has a leader for this shard after a successful STRONG probe.",
			ConstLabels: prometheus.Labels{"shard": strconv.Itoa(i)},
		}, func() float64 { return gauge(group.leader.Load() >= 0) }))
	}
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kvstore_rpc_duration_seconds", Help: "Successful point and batch RPC durations at the router.",
		Buckets: []float64{0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.075, 0.100, 0.150, 0.250, 0.5, 1, 2.5, 5, 10},
	}, []string{"method"})
	rpcs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_rpc_total", Help: "Completed point and batch RPCs by outcome.",
	}, []string{"method", "code"})
	mutations := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_mutations_total", Help: "Logical mutations by caller-visible result, including partially failed batches.",
	}, []string{"outcome"})
	ok, failed := mutations.WithLabelValues("ok"), mutations.WithLabelValues("failed")
	registry.MustRegister(duration, rpcs, mutations)

	observe := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		var method string
		switch info.FullMethod {
		case kv.KVStore_Get_FullMethodName:
			method = "get"
		case kv.KVStore_Set_FullMethodName:
			method = "set"
		case kv.KVStore_Delete_FullMethodName:
			method = "delete"
		case kv.KVStore_BatchWrite_FullMethodName:
			method = "batch_write"
		default:
			return next(ctx, req)
		}
		start := time.Now()
		reply, err := next(ctx, req)
		elapsed := time.Since(start).Seconds()
		rpcs.WithLabelValues(method, status.Code(err).String()).Inc()
		success := err == nil
		switch method {
		case "set", "delete":
			if err == nil {
				ok.Inc()
			} else {
				failed.Inc()
			}
		case "batch_write":
			if err != nil {
				failed.Add(float64(len(req.(*kv.BatchWriteRequest).GetOperations())))
			} else {
				successes := 0
				results := reply.(*kv.BatchWriteResponse).GetResults()
				for _, item := range results {
					if item != nil && codes.Code(item.GetGrpcCode()) == codes.OK {
						successes++
					}
				}
				ok.Add(float64(successes))
				failed.Add(float64(len(results) - successes))
				success = successes == len(results)
			}
		}
		if success {
			duration.WithLabelValues(method).Observe(elapsed)
		}
		return reply, err
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), observe
}

func gauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
