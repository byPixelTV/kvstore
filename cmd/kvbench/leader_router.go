package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// leaderRoutePool is a benchmark-only, allowlisted leader-aware connection pool.
// A production SDK should use the same strategy with authentication/TLS and
// idempotency keys for retryable writes; this benchmark intentionally does NOT
// retry an uncertain write after a failed RPC.
type leaderRoutePool struct {
	bootstrap string
	mapping   string
	timeout   time.Duration
	refresh   time.Duration
	count     int

	mu        sync.RWMutex
	active    *leaderRoute
	fallback  *leaderRoute
	routes    map[string]*leaderRoute
	stop      chan struct{}
	done      chan struct{}
	wake      chan struct{}
	switches  atomic.Uint64
	failovers atomic.Uint64
}

type leaderRoute struct {
	address     string
	connections []*grpc.ClientConn
	clients     []kvstorev1.KVStoreClient
}

func (r *leaderRoute) Close() {
	for _, conn := range r.connections {
		_ = conn.Close()
	}
}

func newLeaderRoutePool(bootstrap, mapping string, count int, timeout, refresh time.Duration) (_ *leaderRoutePool, err error) {
	// Reject invalid or absent addresses before opening any connections.
	if err := validateLeaderMap(mapping); err != nil {
		return nil, err
	}
	if count < 1 {
		return nil, errors.New("at least one connection required")
	}
	r := &leaderRoutePool{
		bootstrap: bootstrap, mapping: mapping, timeout: timeout, refresh: refresh, count: count,
		routes: make(map[string]*leaderRoute), stop: make(chan struct{}), done: make(chan struct{}), wake: make(chan struct{}, 1),
	}
	defer func() {
		if err != nil {
			for _, route := range r.routes {
				route.Close()
			}
		}
	}()
	fallback, err := r.getOrCreate(bootstrap)
	if err != nil {
		return nil, err
	}
	r.fallback = fallback
	r.active = fallback
	// Initial lookup must succeed: otherwise an ostensibly direct-leader run
	// would silently measure Envoy forwarding instead.
	if err = r.Refresh(); err != nil {
		return nil, err
	}
	go r.monitor()
	return r, nil
}

func validateLeaderMap(mapping string) error {
	if strings.TrimSpace(mapping) == "" {
		return errors.New("leader map is empty")
	}
	for _, item := range strings.Split(mapping, ",") {
		pair := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(pair) != 2 || pair[0] == "" || pair[1] == "" {
			return fmt.Errorf("invalid leader map entry %q", item)
		}
		if _, _, err := net.SplitHostPort(pair[1]); err != nil {
			return fmt.Errorf("invalid reachable address %q: %w", pair[1], err)
		}
	}
	return nil
}

// getOrCreate is called only from the constructor and the single monitor
// goroutine. Routes remain open until Close so in-flight RPCs are not severed
// when leadership changes.
func (r *leaderRoutePool) getOrCreate(address string) (*leaderRoute, error) {
	if route := r.routes[address]; route != nil {
		return route, nil
	}
	result := &leaderRoute{address: address}
	for i := 0; i < r.count; i++ {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			result.Close()
			return nil, err
		}
		result.connections = append(result.connections, conn)
		result.clients = append(result.clients, kvstorev1.NewKVStoreClient(conn))
	}
	r.routes[address] = result
	return result, nil
}

func (r *leaderRoutePool) Refresh() error {
	address, err := resolveLeaderAddress(r.bootstrap, r.mapping, r.timeout)
	if err != nil {
		return err
	}
	// This function runs once before monitor start, then only on the monitor.
	route, err := r.getOrCreate(address)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if r.active != route {
		r.active = route
		r.switches.Add(1)
	}
	r.mu.Unlock()
	return nil
}

func (r *leaderRoutePool) monitor() {
	defer close(r.done)
	if r.refresh == 0 {
		<-r.stop
		return
	}
	ticker := time.NewTicker(r.refresh)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
		case <-r.wake:
		}
		_ = r.Refresh() // errors leave the last known route, or fallback, in place
	}
}

func (r *leaderRoutePool) Close() {
	close(r.stop)
	<-r.done
	for _, route := range r.routes {
		route.Close()
	}
}

func (r *leaderRoutePool) activeRoute() *leaderRoute {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.active
}

func (r *leaderRoutePool) target() string { return r.activeRoute().address }

func (r *leaderRoutePool) signalUnavailable(route *leaderRoute) {
	if route == r.fallback {
		return
	}
	r.mu.Lock()
	if r.active == route {
		r.active = r.fallback
		r.failovers.Add(1)
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}
	r.mu.Unlock()
}

func (r *leaderRoutePool) Clients() []kvstorev1.KVStoreClient {
	clients := make([]kvstorev1.KVStoreClient, r.count)
	for i := range clients {
		clients[i] = &leaderRoutedClient{KVStoreClient: r.fallback.clients[i], pool: r, worker: i}
	}
	return clients
}

type leaderRoutedClient struct {
	kvstorev1.KVStoreClient // Streaming calls use the bootstrap/fallback endpoint.
	pool                    *leaderRoutePool
	worker                  int
}

func (c *leaderRoutedClient) Get(ctx context.Context, req *kvstorev1.GetRequest, opts ...grpc.CallOption) (*kvstorev1.GetResponse, error) {
	route := c.pool.activeRoute()
	result, err := route.clients[c.worker].Get(ctx, req, opts...)
	if status.Code(err) == codes.Unavailable && route != c.pool.fallback {
		c.pool.signalUnavailable(route)
		// Reads are side-effect-free, so one fallback attempt is safe.
		if ctx.Err() == nil {
			return c.pool.fallback.clients[c.worker].Get(ctx, req, opts...)
		}
	}
	return result, err
}

func (c *leaderRoutedClient) Set(ctx context.Context, req *kvstorev1.SetRequest, opts ...grpc.CallOption) (*kvstorev1.SetResponse, error) {
	route := c.pool.activeRoute()
	result, err := route.clients[c.worker].Set(ctx, req, opts...)
	if status.Code(err) == codes.Unavailable {
		c.pool.signalUnavailable(route)
	}
	// An unavailable write may have committed. Never blindly retry it.
	return result, err
}

func (c *leaderRoutedClient) Delete(ctx context.Context, req *kvstorev1.DeleteRequest, opts ...grpc.CallOption) (*kvstorev1.DeleteResponse, error) {
	route := c.pool.activeRoute()
	result, err := route.clients[c.worker].Delete(ctx, req, opts...)
	if status.Code(err) == codes.Unavailable {
		c.pool.signalUnavailable(route)
	}
	return result, err
}

func (c *leaderRoutedClient) Head(ctx context.Context, req *kvstorev1.HeadRequest, opts ...grpc.CallOption) (*kvstorev1.HeadResponse, error) {
	route := c.pool.activeRoute()
	result, err := route.clients[c.worker].Head(ctx, req, opts...)
	if status.Code(err) == codes.Unavailable && route != c.pool.fallback {
		c.pool.signalUnavailable(route)
		if ctx.Err() == nil {
			return c.pool.fallback.clients[c.worker].Head(ctx, req, opts...)
		}
	}
	return result, err
}
