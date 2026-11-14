package sharding

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Router is a stateless, version-pinned routing gateway. Each shard is a
// separate THREE-voter Raft cluster, NOT three replicas of one Raft log.
// The router never persists data, allocates revisions or retries writes.
type Router struct {
	kvstorev1.UnimplementedKVStoreServer
	groups [ShardCount]*group
	ready  atomic.Bool
}

type group struct {
	endpoints [3]string
	conns     [3][]*grpc.ClientConn
	clients   [3][]kvstorev1.KVStoreClient
	robin     atomic.Uint64
	leader    atomic.Int32 // -1 until discovery; elected leader only
}

func New(layout Layout, connectionsPerBackend int) (_ *Router, err error) {
	if connectionsPerBackend < 1 || connectionsPerBackend > 32 {
		return nil, errors.New("connections per backend must be 1..32")
	}
	r := &Router{}
	defer func() {
		if err != nil {
			_ = r.Close()
		}
	}()
	for s, endpoints := range layout.Groups {
		g := &group{}
		g.leader.Store(-1)
		r.groups[s] = g
		for i, address := range endpoints {
			g.endpoints[i] = address
			for j := 0; j < connectionsPerBackend; j++ {
				// grpc.NewClient is lazy, so readiness is checked via leader discovery.
				conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if e != nil {
					return nil, fmt.Errorf("shard %d endpoint %d channel %d: %w", s, i, j, e)
				}
				g.conns[i] = append(g.conns[i], conn)
				g.clients[i] = append(g.clients[i], kvstorev1.NewKVStoreClient(conn))
			}
		}
	}
	return r, nil
}
func (r *Router) Close() error {
	var errs []error
	for _, g := range r.groups {
		if g == nil {
			continue
		}
		for _, conns := range g.conns {
			for _, c := range conns {
				if c != nil {
					if err := c.Close(); err != nil {
						errs = append(errs, err)
					}
				}
			}
		}
	}
	return errors.Join(errs...)
}
func (g *group) selectClient() (kvstorev1.KVStoreClient, error) {
	i := g.leader.Load()
	if i < 0 || i >= 3 {
		return nil, status.Error(codes.Unavailable, "shard Raft leader unknown")
	}
	clients := g.clients[i]
	if len(clients) == 0 {
		return nil, status.Error(codes.Unavailable, "shard has no configured client")
	}
	return clients[int(g.robin.Add(1)%uint64(len(clients)))], nil
}

// Discover validates leader metadata against the static allowlist. The leader
// hint is never used as an arbitrary dial target (SSRF protection).
func (g *group) discover(ctx context.Context) error {
	for i, clients := range g.clients {
		if len(clients) == 0 {
			continue
		}
		c := clients[0]
		probe, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		probe = metadata.AppendToOutgoingContext(probe, "kvstore-discover-leader", "1")
		var headers metadata.MD
		_, err := c.Head(probe, &kvstorev1.HeadRequest{}, grpc.Header(&headers))
		cancel()
		if err != nil {
			continue
		}
		hints := headers.Get("kvstore-leader-grpc")
		if len(hints) != 1 {
			continue
		}
		for j, address := range g.endpoints {
			if hints[0] == address {
				g.leader.Store(int32(j))
				return nil
			}
		}
		slog.Warn("ignored Raft leader outside shard allowlist", "backend", g.endpoints[i], "leader", hints[0])
	}
	g.leader.Store(-1)
	return errors.New("no reachable, allowlisted shard leader")
}
func (r *Router) Refresh(ctx context.Context) error {
	var errs []error
	for i, g := range r.groups {
		if err := g.discover(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", i, err))
		}
	}
	r.ready.Store(len(errs) == 0)
	return errors.Join(errs...)
}
func (r *Router) Ready() bool { return r.ready.Load() }

func invalidKey(namespace, key string) error {
	if namespace == "" {
		return status.Error(codes.InvalidArgument, "namespace cannot be empty")
	}
	if key == "" {
		return status.Error(codes.InvalidArgument, "key cannot be empty")
	}
	return nil
}

func (r *Router) Get(ctx context.Context, req *kvstorev1.GetRequest) (*kvstorev1.GetResponse, error) {
	if err := invalidKey(req.GetNamespace(), req.GetKey()); err != nil {
		return nil, err
	}
	id := Shard(req.GetNamespace(), req.GetKey())
	r.routeMetadata(ctx, id)
	g := r.groups[id]
	c, err := g.selectClient()
	if err != nil {
		return nil, err
	}
	result, err := c.Get(ctx, req)
	// Reads may safely be retried only when routing is out-of-date.
	if status.Code(err) == codes.Unavailable && ctx.Err() == nil {
		if e := g.discover(ctx); e == nil {
			if replacement, e := g.selectClient(); e == nil {
				result, err = replacement.Get(ctx, req)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if result.GetFound() {
		v, e := EncodeRevision(id, result.GetRevision())
		if e != nil {
			return nil, status.Error(codes.Internal, e.Error())
		}
		result.Revision = v
	}
	return result, nil
}
func (r *Router) Set(ctx context.Context, req *kvstorev1.SetRequest) (*kvstorev1.SetResponse, error) {
	if err := invalidKey(req.GetNamespace(), req.GetKey()); err != nil {
		return nil, err
	}
	id := Shard(req.GetNamespace(), req.GetKey())
	r.routeMetadata(ctx, id)
	g := r.groups[id]
	c, err := g.selectClient()
	if err != nil {
		return nil, err
	}
	result, err := c.Set(ctx, req)
	if status.Code(err) == codes.Unavailable {
		g.leader.Store(-1)
	}
	// DO NOT retry: failed RPC may have reached quorum and committed.
	if err != nil {
		return nil, err
	}
	v, e := EncodeRevision(id, result.GetRevision())
	if e != nil {
		return nil, status.Error(codes.Internal, e.Error())
	}
	result.Revision = v
	return result, nil
}
func (r *Router) Delete(ctx context.Context, req *kvstorev1.DeleteRequest) (*kvstorev1.DeleteResponse, error) {
	if err := invalidKey(req.GetNamespace(), req.GetKey()); err != nil {
		return nil, err
	}
	id := Shard(req.GetNamespace(), req.GetKey())
	r.routeMetadata(ctx, id)
	g := r.groups[id]
	c, err := g.selectClient()
	if err != nil {
		return nil, err
	}
	result, err := c.Delete(ctx, req)
	if status.Code(err) == codes.Unavailable {
		g.leader.Store(-1)
	}
	if err != nil {
		return nil, err
	}
	v, e := EncodeRevision(id, result.GetRevision())
	if e != nil {
		return nil, status.Error(codes.Internal, e.Error())
	}
	result.Revision = v
	return result, nil
}

func requireV2(active bool) error {
	if active {
		return nil
	}
	return status.Error(codes.FailedPrecondition, "multi-Raft cursors are vectors; regenerate protobuf bindings and use use_vector_cursor=true (legacy global uint64 cursors are unsupported)")
}
func (r *Router) Head(ctx context.Context, req *kvstorev1.HeadRequest) (*kvstorev1.HeadResponse, error) {
	if err := requireV2(req.GetUseVectorCursor()); err != nil {
		return nil, err
	}
	var c Cursor
	for i, g := range r.groups {
		client, err := g.selectClient()
		if err != nil {
			return nil, err
		}
		res, err := client.Head(ctx, &kvstorev1.HeadRequest{})
		if err != nil {
			return nil, err
		}
		c[i] = res.GetRevision()
	}
	// Revision zero is deliberate: no cross-shard global revision exists.
	return &kvstorev1.HeadResponse{Cursor: c.Encode()}, nil
}

// ServeRouter starts the existing KVStore binary in stateless router mode.
// It is intended to be behind an authenticated/TLS proxy for production.
func ServeRouter() error {
	listen := getenv("KVSTORE_LISTEN_ADDR", ":50051")
	if _, _, err := net.SplitHostPort(listen); err != nil {
		return fmt.Errorf("router listen address: %w", err)
	}
	layout, err := ParseLayout(os.Getenv("KVSTORE_SHARDS"))
	if err != nil {
		return err
	}
	if raw := getenv("KVSTORE_SHARD_LAYOUT_VERSION", "1"); raw != "1" {
		return fmt.Errorf("unsupported KVSTORE_SHARD_LAYOUT_VERSION %q", raw)
	}
	refresh, err := time.ParseDuration(getenv("KVSTORE_SHARD_DISCOVERY_INTERVAL", "2s"))
	if err != nil || refresh < 100*time.Millisecond {
		return errors.New("discovery interval must be at least 100ms")
	}
	connections, err := strconv.Atoi(getenv("KVSTORE_ROUTER_CONNECTIONS", "8"))
	if err != nil {
		return fmt.Errorf("KVSTORE_ROUTER_CONNECTIONS: %w", err)
	}
	router, err := New(layout, connections)
	if err != nil {
		return err
	}
	defer router.Close()
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := grpc.NewServer()
	kvstorev1.RegisterKVStoreServer(server, router)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(refresh)
		defer ticker.Stop()
		for {
			checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := router.Refresh(checkCtx)
			cancel()
			state := healthpb.HealthCheckResponse_NOT_SERVING
			if err == nil {
				state = healthpb.HealthCheckResponse_SERVING
			}
			healthServer.SetServingStatus("readiness", state)
			healthServer.SetServingStatus("", state)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- server.Serve(listener) }()
	slog.Info("sharded router started", "listen", listen, "shards", ShardCount, "layout_version", LayoutVersion)
	select {
	case err := <-errorsCh:
		stop()
		<-done
		return err
	case <-ctx.Done():
		healthServer.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
		finished := make(chan struct{})
		go func() { server.GracefulStop(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(15 * time.Second):
			server.Stop()
			<-finished
		}
		<-errorsCh
		<-done
		return nil
	}
}
func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// The version and routing layout are emitted as metadata for diagnostic SDKs.
func (r *Router) routeMetadata(ctx context.Context, shard int) {
	_ = grpc.SetHeader(ctx, metadata.Pairs("kvstore-shard", strconv.Itoa(shard), "kvstore-shard-layout", fmt.Sprint(LayoutVersion)))
}

var _ kvstorev1.KVStoreServer = (*Router)(nil)
