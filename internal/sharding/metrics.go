package sharding

import (
	"net/http"
	"strconv"

	"github.com/byPixelTV/kvstore/internal/monitoring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
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
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), monitoring.RegisterRPC(registry)
}

func gauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
