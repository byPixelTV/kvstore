// kvbench measures end-to-end gRPC performance, not Pebble's raw I/O speed.
// Uses the same protobuf API for standalone and Raft cluster deployments.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const sampleSize = 20000

type latency struct {
	Count  int64   `json:"count"`
	Sample []int64 `json:"-"`
}

func (l *latency) Add(ns int64, rng *mathrand.Rand) {
	l.Count++
	if len(l.Sample) < sampleSize {
		l.Sample = append(l.Sample, ns)
		return
	}
	if p := rng.Int63n(l.Count); p < sampleSize {
		l.Sample[p] = ns
	}
}
func (l *latency) Merge(other latency) {
	l.Count += other.Count
	l.Sample = append(l.Sample, other.Sample...)
}

type percentile struct {
	P50ms   float64 `json:"p50_ms"`
	P95ms   float64 `json:"p95_ms"`
	P99ms   float64 `json:"p99_ms"`
	Maxms   float64 `json:"max_ms"`
	Sampled int     `json:"latency_samples"`
}

func (l latency) Percentiles() percentile {
	if len(l.Sample) == 0 {
		return percentile{}
	}
	sort.Slice(l.Sample, func(i, j int) bool { return l.Sample[i] < l.Sample[j] })
	get := func(p float64) float64 {
		at := int(math.Ceil(p*float64(len(l.Sample)))) - 1
		if at < 0 {
			at = 0
		}
		return float64(l.Sample[at]) / 1e6
	}
	return percentile{P50ms: get(.5), P95ms: get(.95), P99ms: get(.99), Maxms: get(1), Sampled: len(l.Sample)}
}

type aggregate struct {
	OK       int64   `json:"ok"`
	Errors   int64   `json:"errors"`
	NotFound int64   `json:"not_found"`
	Lat      latency `json:"-"`
}

func (a *aggregate) Merge(b aggregate) {
	a.OK += b.OK
	a.Errors += b.Errors
	a.NotFound += b.NotFound
	a.Lat.Merge(b.Lat)
}

type operationReport struct {
	OK       int64      `json:"ok"`
	Errors   int64      `json:"errors"`
	NotFound int64      `json:"not_found"`
	OpsSec   float64    `json:"ops_sec"`
	Latency  percentile `json:"latency"`
}

func (a aggregate) Report(seconds float64) operationReport {
	if seconds <= 0 {
		seconds = 1
	}
	return operationReport{OK: a.OK, Errors: a.Errors, NotFound: a.NotFound, OpsSec: float64(a.OK) / seconds, Latency: a.Lat.Percentiles()}
}

type report struct {
	Mode                string          `json:"mode"`
	Consistency         string          `json:"consistency"`
	Address             string          `json:"address"`
	ResolvedAddress     string          `json:"resolved_address,omitempty"`
	Namespace           string          `json:"namespace"`
	DurationSeconds     float64         `json:"duration_seconds"`
	WarmupSeconds       float64         `json:"warmup_seconds"`
	Concurrency         int             `json:"concurrency"`
	Connections         int             `json:"connections"`
	Keyspace            int             `json:"keyspace"`
	ValueBytes          int             `json:"value_bytes"`
	CPUs                int             `json:"logical_cpus"`
	LeaderRouteSwitches uint64          `json:"leader_route_switches,omitempty"`
	LeaderFailovers     uint64          `json:"leader_failovers,omitempty"`
	Get                 operationReport `json:"get"`
	Set                 operationReport `json:"set"`
	Watch               *watchReport    `json:"watch,omitempty"`
}
type watchReport struct {
	Subscribers      int        `json:"subscribers"`
	WriteRateTarget  int        `json:"write_rate_target"`
	Deliveries       int64      `json:"deliveries"`
	DeliveriesPerSec float64    `json:"deliveries_per_sec"`
	DeliveryLatency  percentile `json:"delivery_latency"`
}
type options struct {
	address       string
	mode          string
	consistency   string
	duration      time.Duration
	warmup        time.Duration
	concurrency   int
	connections   int
	keys          int
	valueSize     int
	readPercent   int
	timeout       time.Duration
	watchers      int
	watchRate     int
	jsonOutput    bool
	leaderMap     string
	leaderRefresh time.Duration
}

func main() {
	opt := options{}
	flag.StringVar(&opt.address, "addr", "127.0.0.1:50051", "KVStore gRPC endpoint")
	flag.StringVar(&opt.mode, "mode", "mixed", "get, set, mixed, or watch")
	flag.StringVar(&opt.consistency, "consistency", "strong", "strong or eventual for GET requests")
	flag.DurationVar(&opt.duration, "duration", 15*time.Second, "measured duration")
	flag.DurationVar(&opt.warmup, "warmup", 3*time.Second, "unmeasured warmup duration")
	flag.IntVar(&opt.concurrency, "concurrency", 16, "parallel RPC workers (get/set/mixed)")
	flag.IntVar(&opt.connections, "connections", 4, "independent HTTP/2 gRPC connections")
	flag.IntVar(&opt.keys, "keys", 1000, "number of distinct benchmark keys")
	flag.IntVar(&opt.valueSize, "value-size", 256, "value payload size in bytes")
	flag.IntVar(&opt.readPercent, "read-percent", 80, "percentage of reads in mixed mode")
	flag.DurationVar(&opt.timeout, "timeout", 8*time.Second, "per-RPC timeout")
	flag.IntVar(&opt.watchers, "watchers", 5, "concurrent Watch subscribers in watch mode")
	flag.IntVar(&opt.watchRate, "watch-rate", 100, "target writes/second in watch mode")
	flag.StringVar(&opt.leaderMap, "leader-map", "", "route directly to Raft leader using gRPC metadata; map advertised=reachable targets, comma-separated")
	flag.DurationVar(&opt.leaderRefresh, "leader-refresh", 2*time.Second, "leader re-discovery interval when -leader-map is set; 0 disables periodic refresh")
	flag.BoolVar(&opt.jsonOutput, "json", false, "write JSON report to standard output")
	flag.Parse()
	if !valid(opt) {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(opt); err != nil {
		fmt.Fprintln(os.Stderr, "kvbench:", err)
		os.Exit(1)
	}
}
func valid(o options) bool {
	return (o.consistency == "strong" || o.consistency == "eventual") && (o.mode == "get" || o.mode == "set" || o.mode == "mixed" || o.mode == "watch") && o.duration > 0 && o.warmup >= 0 && o.connections > 0 && o.concurrency > 0 && o.keys > 0 && o.valueSize >= 8 && o.valueSize <= 256<<10 && o.readPercent >= 0 && o.readPercent <= 100 && o.timeout > 0 && o.leaderRefresh >= 0 && o.watchers > 0 && o.watchRate > 0 && o.watchRate <= 100000
}

// resolveLeaderAddress asks the load balancer for the Raft leader and maps
// the advertised Docker/private address to a reachable address. No arbitrary
// target from untrusted gRPC metadata is dialed: the mapping is explicit.
func resolveLeaderAddress(bootstrap, mapping string, timeout time.Duration) (string, error) {
	mappings := make(map[string]string)
	for _, item := range strings.Split(mapping, ",") {
		pair := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(pair) != 2 || pair[0] == "" || pair[1] == "" {
			return "", fmt.Errorf("invalid -leader-map entry %q (expected advertised=reachable)", item)
		}
		mappings[pair[0]] = pair[1]
	}
	conn, err := grpc.NewClient(bootstrap, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var hdr metadata.MD
	ctx = metadata.AppendToOutgoingContext(ctx, "kvstore-discover-leader", "1")
	_, err = kvstorev1.NewKVStoreClient(conn).Head(ctx, &kvstorev1.HeadRequest{}, grpc.Header(&hdr))
	if err != nil {
		return "", fmt.Errorf("leader discovery Head: %w", err)
	}
	advertised := hdr.Get("kvstore-leader-grpc")
	if len(advertised) == 0 || advertised[0] == "" {
		return "", errors.New("no leader metadata from cluster endpoint")
	}
	if target := mappings[advertised[0]]; target != "" {
		return target, nil
	}
	return "", fmt.Errorf("Raft leader %q missing from -leader-map", advertised[0])
}

func run(opt options) error {
	var runID [8]byte
	if _, err := rand.Read(runID[:]); err != nil {
		return err
	}
	namespace := fmt.Sprintf("kvbench-%x", runID)
	target := opt.address
	var pool *leaderRoutePool
	clients := make([]kvstorev1.KVStoreClient, 0, opt.connections)
	if opt.leaderMap != "" {
		var err error
		pool, err = newLeaderRoutePool(opt.address, opt.leaderMap, opt.connections, opt.timeout, opt.leaderRefresh)
		if err != nil {
			return err
		}
		defer pool.Close()
		target = pool.target()
		clients = pool.Clients()
		fmt.Fprintf(os.Stderr, "kvbench: initial leader %s -> %s (refresh %s)\n", opt.address, target, opt.leaderRefresh)
	} else {
		connections := make([]*grpc.ClientConn, 0, opt.connections)
		defer func() {
			for _, c := range connections {
				_ = c.Close()
			}
		}()
		for i := 0; i < opt.connections; i++ {
			conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			connections = append(connections, conn)
			clients = append(clients, kvstorev1.NewKVStoreClient(conn))
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), opt.timeout)
	_, err := clients[0].Get(ctx, &kvstorev1.GetRequest{Namespace: namespace, Key: "__kvbench_connectivity_probe", Consistency: kvstorev1.ReadConsistency_READ_CONSISTENCY_STRONG})
	cancel()
	if err != nil {
		return fmt.Errorf("GET connectivity check: %w", err)
	}
	rep := report{Mode: opt.mode, Consistency: opt.consistency, Address: opt.address, ResolvedAddress: target, Namespace: namespace, DurationSeconds: opt.duration.Seconds(), WarmupSeconds: opt.warmup.Seconds(), Concurrency: opt.concurrency, Connections: opt.connections, Keyspace: opt.keys, ValueBytes: opt.valueSize, CPUs: runtime.NumCPU()}
	if opt.mode == "watch" {
		err = runWatch(opt, clients, namespace, &rep)
	} else {
		err = runRPC(opt, clients, namespace, &rep)
	}
	if err != nil {
		return err
	}
	if pool != nil {
		rep.ResolvedAddress = pool.target()
		rep.LeaderRouteSwitches = pool.switches.Load()
		rep.LeaderFailovers = pool.failovers.Load()
	}
	if opt.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	fmt.Printf("kvbench %s • %s • %d workers • %d HTTP/2 connections • %d B/value\n", rep.Mode, rep.Address, rep.Concurrency, rep.Connections, rep.ValueBytes)
	printOp := func(label string, o operationReport) {
		if o.OK+o.Errors == 0 {
			return
		}
		fmt.Printf("%-6s %10.0f ops/s   ok=%d err=%d not_found=%d   p50=%8.3fms p95=%8.3fms p99=%8.3fms (sample=%d)\n", label, o.OpsSec, o.OK, o.Errors, o.NotFound, o.Latency.P50ms, o.Latency.P95ms, o.Latency.P99ms, o.Latency.Sampled)
	}
	printOp("GET", rep.Get)
	printOp("SET", rep.Set)
	if rep.Watch != nil {
		w := rep.Watch
		fmt.Printf("WATCH  %10.0f events/s (across %d subscribers), p50=%8.3fms p95=%8.3fms p99=%8.3fms\n", w.DeliveriesPerSec, w.Subscribers, w.DeliveryLatency.P50ms, w.DeliveryLatency.P95ms, w.DeliveryLatency.P99ms)
	}
	fmt.Printf("Namespace: %s (benchmark data is retained)\n", namespace)
	return nil
}
func keyAt(i int) string { return fmt.Sprintf("key-%07d", i) }
func fill(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}
func rpcContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}
func write(client kvstorev1.KVStoreClient, opt options, namespace, key string, value []byte) error {
	ctx, cancel := rpcContext(opt.timeout)
	defer cancel()
	_, err := client.Set(ctx, &kvstorev1.SetRequest{Namespace: namespace, Key: key, Value: value})
	return err
}

// Prefill must not dominate a short benchmark. Bounded parallel writes
// populate the test namespace, but all complete before the timed workload.
func prefill(opt options, clients []kvstorev1.KVStoreClient, namespace string) error {
	if opt.mode == "set" {
		return nil
	}
	value := fill(opt.valueSize)
	workers := opt.concurrency
	if workers > 64 {
		workers = 64
	}
	if workers > opt.keys {
		workers = opt.keys
	}
	var next atomic.Int64
	var failed atomic.Bool
	var firstErr error
	var once sync.Once
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := clients[id%len(clients)]
			for !failed.Load() {
				key := int(next.Add(1)) - 1
				if key >= opt.keys {
					return
				}
				if err := write(client, opt, namespace, keyAt(key), value); err != nil {
					once.Do(func() { firstErr = fmt.Errorf("prefill key %d: %w", key, err); failed.Store(true) })
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	return firstErr
}
func runRPC(opt options, clients []kvstorev1.KVStoreClient, namespace string, rep *report) error {
	if err := prefill(opt, clients, namespace); err != nil {
		return err
	}
	start := time.Now()
	measureFrom := start.Add(opt.warmup)
	end := measureFrom.Add(opt.duration)
	type partial struct{ get, set aggregate }
	result := make(chan partial, opt.concurrency)
	var wg sync.WaitGroup
	for worker := 0; worker < opt.concurrency; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := mathrand.New(mathrand.NewSource(int64(731 + id*127)))
			cl := clients[id%len(clients)]
			value := fill(opt.valueSize)
			p := partial{}
			for time.Now().Before(end) {
				key := keyAt(rng.Intn(opt.keys))
				isRead := opt.mode == "get" || (opt.mode == "mixed" && rng.Intn(100) < opt.readPercent)
				if opt.mode == "set" {
					isRead = false
				}
				ctx, cancel := rpcContext(opt.timeout)
				begun := time.Now()
				var err error
				missing := false
				if isRead {
					consistency := kvstorev1.ReadConsistency_READ_CONSISTENCY_STRONG
					if opt.consistency == "eventual" {
						consistency = kvstorev1.ReadConsistency_READ_CONSISTENCY_EVENTUAL
					}
					res, readErr := cl.Get(ctx, &kvstorev1.GetRequest{Namespace: namespace, Key: key, Consistency: consistency})
					err = readErr
					missing = readErr == nil && !res.GetFound()
				} else {
					_, err = cl.Set(ctx, &kvstorev1.SetRequest{Namespace: namespace, Key: key, Value: value})
				}
				elapsed := time.Since(begun)
				cancel()
				if begun.Before(measureFrom) {
					continue
				}
				target := &p.set
				if isRead {
					target = &p.get
				}
				if err != nil {
					target.Errors++
				} else {
					target.OK++
					if isRead && missing {
						target.NotFound++
					}
					target.Lat.Add(elapsed.Nanoseconds(), rng)
				}
			}
			result <- p
		}(worker)
	}
	wg.Wait()
	close(result)
	var get, set aggregate
	for partial := range result {
		get.Merge(partial.get)
		set.Merge(partial.set)
	}
	rep.Get = get.Report(opt.duration.Seconds())
	rep.Set = set.Report(opt.duration.Seconds())
	return nil
}
func runWatch(opt options, clients []kvstorev1.KVStoreClient, namespace string, rep *report) error {
	ctx, cancel := context.WithTimeout(context.Background(), opt.duration+opt.warmup+opt.timeout+5*time.Second)
	defer cancel()
	head, err := clients[0].Head(ctx, &kvstorev1.HeadRequest{})
	if err != nil {
		return err
	}
	type watcher struct {
		stream grpc.ServerStreamingClient[kvstorev1.WatchResponse]
		result chan latency
		count  chan int64
	}
	subscriptions := make([]watcher, 0, opt.watchers)
	for i := 0; i < opt.watchers; i++ {
		stream, err := clients[i%len(clients)].Watch(ctx, &kvstorev1.WatchRequest{Namespace: namespace, AfterRevision: head.GetRevision()})
		if err != nil {
			return err
		}
		first, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("watcher %d could not start: %w", i, err)
		}
		if !first.GetCheckpoint() {
			return errors.New("Watch did not send initial cursor checkpoint")
		}
		subscriptions = append(subscriptions, watcher{stream: stream, result: make(chan latency, 1)})
	}
	started := time.Now()
	measureFrom := started.Add(opt.warmup)
	end := measureFrom.Add(opt.duration)
	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i, w := range subscriptions {
		wg.Add(1)
		go func(id int, w watcher) {
			defer wg.Done()
			l := latency{}
			rng := mathrand.New(mathrand.NewSource(int64(941 + id)))
			for {
				msg, err := w.stream.Recv()
				if err != nil {
					if err != io.EOF && runCtx.Err() == nil {
						fmt.Fprintln(os.Stderr, "watch stream ended:", err)
					}
					break
				}
				if msg.GetCheckpoint() || msg.GetDeleted() || len(msg.GetValue()) < 8 {
					continue
				}
				sent := int64(binary.BigEndian.Uint64(msg.GetValue()[:8]))
				if sent < measureFrom.UnixNano() {
					continue
				}
				ns := time.Now().UnixNano() - sent
				if ns >= 0 {
					l.Add(ns, rng)
				}
			}
			w.result <- l
		}(i, w)
	}
	// A single paced writer produces changes for all subscribers. Each event's
	// first eight value bytes carry the local Unix timestamp for delivery lag.
	value := fill(opt.valueSize)
	tick := time.NewTicker(time.Second / time.Duration(opt.watchRate))
	defer tick.Stop()
	var writes aggregate
	rng := mathrand.New(mathrand.NewSource(889))
	for time.Now().Before(end) {
		<-tick.C
		binary.BigEndian.PutUint64(value[:8], uint64(time.Now().UnixNano()))
		begun := time.Now()
		err := write(clients[0], opt, namespace, keyAt(rng.Intn(opt.keys)), value)
		if begun.Before(measureFrom) {
			continue
		}
		if err != nil {
			writes.Errors++
		} else {
			writes.OK++
			writes.Lat.Add(time.Since(begun).Nanoseconds(), rng)
		}
	}
	// Give subscribers a bounded period to receive already committed writes.
	time.Sleep(350 * time.Millisecond)
	stop()
	cancel()
	wg.Wait()
	var received latency
	for _, w := range subscriptions {
		received.Merge(<-w.result)
	}
	rep.Set = writes.Report(opt.duration.Seconds())
	rep.Watch = &watchReport{Subscribers: opt.watchers, WriteRateTarget: opt.watchRate, Deliveries: received.Count, DeliveriesPerSec: float64(received.Count) / opt.duration.Seconds(), DeliveryLatency: received.Percentiles()}
	return nil
}
