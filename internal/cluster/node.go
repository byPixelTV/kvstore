package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/changefeed"
	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/byPixelTV/kvstore/internal/storage"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const opTimeout = 10 * time.Second

// Node implements storage.Store while routing writes and strong reads to Raft's
// current leader. Peers never write directly to Pebble outside FSM.Apply.
type Node struct {
	cfg         config.Config
	raft        *raft.Raft
	fsm         *FSM
	raftStore   *raftboltdb.BoltStore
	logStats    *raftLogMetrics
	transport   *raft.NetworkTransport
	admin       *http.Server
	clientsMu   sync.Mutex
	clients     map[string]*grpc.ClientConn
	reads       *readFenceCoordinator
	writes      *clusterWriteBatcher
	readStats   readMetrics
	leaderReads leaderFence
}

var _ storage.Store = (*Node)(nil)

func Open(cfg config.Config) (_ *Node, err error) {
	if cfg.Mode != config.ModeCluster {
		return nil, errors.New("cluster node requires cluster mode")
	}
	fsm, err := openFSM(filepath.Join(cfg.DataDir, "state"))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = fsm.Close()
		}
	}()
	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err = os.MkdirAll(raftDir, 0700); err != nil {
		return nil, err
	}
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = bolt.Close()
		}
	}()
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(raftDir, "snapshots"), 3, os.Stderr)
	if err != nil {
		return nil, err
	}
	advertise, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve Raft advertise address: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftBindAddr, advertise, 4, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = transport.Close()
		}
	}()
	rCfg := raft.DefaultConfig()
	rCfg.LocalID = raft.ServerID(cfg.NodeID)
	rCfg.LogLevel = "INFO"
	// Raft can pass up to MaxAppendEntries committed logs to BatchingFSM.
	// BatchApplyCh allows the leader to fill larger groups under load.
	rCfg.BatchApplyCh = true
	if cfg.RaftMaxAppendEntries > 0 {
		rCfg.MaxAppendEntries = cfg.RaftMaxAppendEntries
	}
	if cfg.RaftCommitTimeout > 0 {
		rCfg.CommitTimeout = cfg.RaftCommitTimeout
	}
	rCfg.SnapshotThreshold = 8192 // avoid snapshot churn during sustained writes
	rCfg.SnapshotInterval = 2 * time.Minute
	existing, err := raft.HasExistingState(bolt, bolt, snaps)
	if err != nil {
		return nil, err
	}
	// Recently appended log entries are re-read for follower replication.
	// Cache only reads; every StoreLogs still passes through to durable BoltDB.
	logMetrics := &raftLogMetrics{}
	var logStore raft.LogStore = &measuredLogStore{inner: bolt, stats: logMetrics}
	if cfg.RaftLogCacheEntries != 0 {
		entries := cfg.RaftLogCacheEntries
		if entries < 0 {
			entries = 512
		} // manually constructed test config
		cache, e := raft.NewLogCache(entries, logStore)
		if e != nil {
			return nil, fmt.Errorf("create Raft log cache: %w", e)
		}
		logStore = cache
	}
	r, err := raft.NewRaft(rCfg, fsm, logStore, bolt, snaps, transport)
	if err != nil {
		return nil, err
	}
	if cfg.Bootstrap && !existing {
		future := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(cfg.NodeID), Address: raft.ServerAddress(cfg.RaftAdvertiseAddr), Suffrage: raft.Voter}}})
		if err = future.Error(); err != nil {
			_ = r.Shutdown().Error()
			return nil, fmt.Errorf("bootstrap raft: %w", err)
		}
	}
	node := &Node{cfg: cfg, raft: r, fsm: fsm, raftStore: bolt, transport: transport, clients: make(map[string]*grpc.ClientConn)}
	node.logStats = logMetrics
	batchWindow := cfg.ReadBatchWindow
	if batchWindow == 0 { // support tests and manually constructed Config values
		batchWindow = 300 * time.Microsecond
	}
	batchMax := cfg.ReadBatchMax
	if batchMax == 0 {
		batchMax = 256
	}
	node.leaderReads = leaderFence{
		term: node.raftTerm,
		barrier: func() error {
			begin := time.Now()
			err := node.raft.Barrier(opTimeout).Error()
			node.readStats.barrierCalls.Add(1)
			node.readStats.barrierNanos.Add(uint64(time.Since(begin).Nanoseconds()))
			return err
		},
		verify: func() error {
			begin := time.Now()
			err := node.raft.VerifyLeader().Error()
			node.readStats.verifyCalls.Add(1)
			node.readStats.verifyNanos.Add(uint64(time.Since(begin).Nanoseconds()))
			return err
		},
	}
	readVerifyParallelism := cfg.ReadVerifyParallelism
	if readVerifyParallelism <= 0 {
		readVerifyParallelism = 2
	}
	node.reads = newReadFenceCoordinatorWithLimit(batchWindow, batchMax, readVerifyParallelism, node.strongFence, &node.readStats)
	node.writes = newClusterWriteBatcher(node)
	return node, nil
}

func (n *Node) RaftState() string { return n.raft.State().String() }
func (n *Node) Ready() bool {
	if n.fsm.Failure() != nil {
		return false
	}
	if n.raft.State() == raft.Shutdown {
		return false
	}
	leader, _ := n.raft.LeaderWithID()
	return leader != ""
}
func (n *Node) SelfPeer() peerInfo {
	return peerInfo{ID: n.cfg.NodeID, RaftAddr: n.cfg.RaftAdvertiseAddr, GRPCAddr: n.cfg.GRPCAdvertiseAddr, AdminAddr: n.cfg.AdminAdvertiseAddr}
}

func (n *Node) Close() error {
	if n.admin != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = n.admin.Shutdown(ctx)
		cancel()
	}
	// Drain accepted write groups BEFORE shutting down Raft.
	n.writes.Close()
	err := n.raft.Shutdown().Error()
	n.reads.Close()
	n.clientsMu.Lock()
	for _, c := range n.clients {
		_ = c.Close()
	}
	n.clientsMu.Unlock()
	if e := n.transport.Close(); e != nil {
		err = errors.Join(err, e)
	}
	if e := n.raftStore.Close(); e != nil {
		err = errors.Join(err, e)
	}
	if e := n.fsm.Close(); e != nil {
		err = errors.Join(err, e)
	}
	return err
}

// FenceSlot is replicated through Raft, preserving ordering with all writes.
// After a seal is confirmed, all later commands for the slot are rejected by
// the FSM (not only by the router). This prevents unknown-outcome writes from
// silently appearing on the old owner after a migration cutover.
func (n *Node) FenceSlot(ctx context.Context, namespace string, slot uint16, sealed bool) error {
	if namespace == "" || slot >= sharding.SlotCount {
		return status.Error(codes.InvalidArgument, "invalid namespace/slot")
	}
	if n.raft.State() != raft.Leader {
		peer, err := n.leaderPeer()
		if err != nil {
			return err
		}
		if peer.ID == n.cfg.NodeID {
			return status.Error(codes.Unavailable, "leader transition")
		}
		client, err := n.client(peer.GRPCAddr)
		if err != nil {
			return err
		}
		authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+n.cfg.JoinToken)
		_, err = client.FenceSlot(authCtx, &kvstorev1.FenceSlotRequest{Namespace: namespace, Slot: uint32(slot), Sealed: sealed})
		return err
	}
	_, err := n.applyDirect(ctx, command{Operation: opFence, Namespace: namespace, Slot: uint32(slot), Sealed: sealed})
	return err
}

func (n *Node) apply(ctx context.Context, cmd command) (uint64, error) {
	if cmd.Operation == opSet || cmd.Operation == opDelete {
		return n.writes.Submit(ctx, cmd)
	}
	result, err := n.applyDirect(ctx, cmd)
	return result.Revision, err
}

func (n *Node) applyDirect(ctx context.Context, cmd command) (applyResult, error) {
	if err := ctx.Err(); err != nil {
		return applyResult{}, err
	}
	raw, err := json.Marshal(cmd)
	if err != nil {
		return applyResult{}, err
	}
	future := n.raft.Apply(raw, opTimeout)
	done := make(chan error, 1)
	go func() { done <- future.Error() }()
	select {
	case <-ctx.Done():
		return applyResult{}, ctx.Err() // may still commit after timeout
	case err := <-done:
		if err != nil {
			return applyResult{}, err
		}
		result, ok := future.Response().(applyResult)
		if !ok {
			return applyResult{}, fmt.Errorf("unexpected FSM result: %T", future.Response())
		}
		return result, result.Err
	}
}

func (n *Node) leaderPeer() (peerInfo, error) {
	leaderAddr, leaderID := n.raft.LeaderWithID()
	if leaderAddr == "" || leaderID == "" {
		return peerInfo{}, status.Error(codes.Unavailable, "no elected Raft leader")
	}
	if string(leaderID) == n.cfg.NodeID {
		return n.SelfPeer(), nil
	}
	peer, ok, err := n.fsm.Peer(string(leaderID))
	if err != nil {
		return peerInfo{}, err
	}
	if !ok {
		return peerInfo{}, status.Errorf(codes.Unavailable, "leader %s has no replicated routing metadata", leaderID)
	}
	return peer, nil
}

func (n *Node) client(target string) (kvstorev1.KVStoreClient, error) {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	if conn := n.clients[target]; conn != nil {
		return kvstorev1.NewKVStoreClient(conn), nil
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	n.clients[target] = conn
	return kvstorev1.NewKVStoreClient(conn), nil
}

// strongFence proves quorum leadership for each batch, while the expensive
// Raft log barrier is executed only once per leadership term. The first
// barrier ensures prior-term commits have reached the local FSM.
func (n *Node) strongFence() error {
	if err := n.leaderReads.Fence(); err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	return nil
}

// LeaderGRPCAddr returns an *advisory* leader endpoint for clients that can
// reach the cluster's internal gRPC network. It is not an authorization token.
func (n *Node) LeaderGRPCAddr() string {
	peer, err := n.leaderPeer()
	if err != nil {
		return ""
	}
	return peer.GRPCAddr
}

func (n *Node) readStrong(ctx context.Context) error {
	if n.fsm.Failure() != nil {
		return n.fsm.Failure()
	}
	return n.reads.Wait(ctx)
}

func (n *Node) localGet(ctx context.Context, namespace, key string) (storage.Record, bool, error) {
	started := time.Now()
	record, found, err := n.fsm.Get(ctx, namespace, key)
	n.readStats.localReads.Add(1)
	n.readStats.localNanos.Add(uint64(time.Since(started).Nanoseconds()))
	return record, found, err
}

// GetLocal is the explicitly opt-in, potentially stale follower read.
func (n *Node) GetLocal(ctx context.Context, namespace, key string) (storage.Record, bool, error) {
	n.readStats.eventualReads.Add(1)
	return n.localGet(ctx, namespace, key)
}

func (n *Node) Get(ctx context.Context, namespace, key string) (storage.Record, bool, error) {
	if n.fsm.Failure() != nil {
		return storage.Record{}, false, n.fsm.Failure()
	}
	if n.raft.State() != raft.Leader {
		leader, err := n.leaderPeer()
		if err != nil {
			return storage.Record{}, false, err
		}
		if leader.ID == n.cfg.NodeID {
			return storage.Record{}, false, status.Error(codes.Unavailable, "leader transition")
		}
		client, err := n.client(leader.GRPCAddr)
		if err != nil {
			return storage.Record{}, false, err
		}
		started := time.Now()
		res, err := client.Get(ctx, &kvstorev1.GetRequest{Namespace: namespace, Key: key})
		n.readStats.forwardReads.Add(1)
		n.readStats.forwardNanos.Add(uint64(time.Since(started).Nanoseconds()))
		if err != nil {
			return storage.Record{}, false, err
		}
		return storage.Record{Value: res.GetValue(), Revision: res.GetRevision()}, res.GetFound(), nil
	}
	if err := n.readStrong(ctx); err != nil {
		return storage.Record{}, false, err
	}
	return n.localGet(ctx, namespace, key)
}

func (n *Node) ReadPerformance() ReadPerformance {
	return n.readStats.Snapshot()
}

func (n *Node) Set(ctx context.Context, namespace, key string, value []byte) (uint64, error) {
	// Keep raft entries under HashiCorp Raft's recommended 512 KiB (JSON base64 adds overhead).
	if len(value) > 256<<10 {
		return 0, status.Error(codes.ResourceExhausted, "cluster values must be at most 256 KiB")
	}
	if n.raft.State() != raft.Leader {
		peer, err := n.leaderPeer()
		if err != nil {
			return 0, err
		}
		if peer.ID == n.cfg.NodeID {
			return 0, status.Error(codes.Unavailable, "leader transition")
		}
		client, err := n.client(peer.GRPCAddr)
		if err != nil {
			return 0, err
		}
		res, err := client.Set(ctx, &kvstorev1.SetRequest{Namespace: namespace, Key: key, Value: value})
		if err != nil {
			return 0, err
		}
		return res.GetRevision(), nil
	}
	return n.apply(ctx, command{Operation: opSet, Namespace: namespace, Key: key, Value: value})
}

func (n *Node) Delete(ctx context.Context, namespace, key string) (uint64, error) {
	if n.raft.State() != raft.Leader {
		peer, err := n.leaderPeer()
		if err != nil {
			return 0, err
		}
		if peer.ID == n.cfg.NodeID {
			return 0, status.Error(codes.Unavailable, "leader transition")
		}
		client, err := n.client(peer.GRPCAddr)
		if err != nil {
			return 0, err
		}
		res, err := client.Delete(ctx, &kvstorev1.DeleteRequest{Namespace: namespace, Key: key})
		if err != nil {
			return 0, err
		}
		return res.GetRevision(), nil
	}
	return n.apply(ctx, command{Operation: opDelete, Namespace: namespace, Key: key})
}

// ensureRegistered replicates the bootstrap node's own gRPC/admin addresses.
// Later elected leaders already have an entry from their join operation.
func (n *Node) ensureRegistered(ctx context.Context) error {
	peer := n.SelfPeer()
	current, ok, err := n.fsm.Peer(peer.ID)
	if err != nil {
		return err
	}
	if ok && current == peer {
		return nil
	}
	_, err = n.apply(ctx, command{Operation: opPeer, Peer: &peer})
	return err
}

func (n *Node) member() bool {
	fut := n.raft.GetConfiguration()
	if fut.Error() != nil {
		return false
	}
	for _, peer := range fut.Configuration().Servers {
		if string(peer.ID) == n.cfg.NodeID {
			return true
		}
	}
	return false
}

// JoinUntilMember requests membership from seeds until added or shutdown.
// The first voter is explicitly bootstrapped; joiners NEVER self-bootstrap.
func (n *Node) JoinUntilMember(ctx context.Context) {
	if n.cfg.Bootstrap {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if n.member() {
			slog.Info("raft membership ready", "node", n.cfg.NodeID)
			return
		}
		for _, seed := range n.cfg.ClusterSeeds {
			if err := n.tryJoin(ctx, seed); err != nil {
				slog.Debug("join seed unavailable", "seed", seed, "error", err)
			}
			if n.member() {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (n *Node) tryJoin(ctx context.Context, seed string) error {
	body, err := json.Marshal(n.SelfPeer())
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, "http://"+seed+"/v1/cluster/join", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+n.cfg.JoinToken)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("join %s: %s", seed, res.Status)
	}
	return nil
}

// Watch on followers is served from their locally applied *committed* Raft log.
// The subscriber's cursor allows reconnection to any other healthy node.
func (n *Node) ReadChanges(ctx context.Context, after uint64, limit int) (changefeed.Page, error) {
	return n.fsm.ReadChanges(ctx, after, limit)
}
func (n *Node) Notify() <-chan struct{} { return n.fsm.Notify() }

// Head is quorum-verified, unlike the local follower stream checkpoint.
func (n *Node) Head(ctx context.Context) (uint64, uint64, error) {
	if n.raft.State() != raft.Leader {
		peer, err := n.leaderPeer()
		if err != nil {
			return 0, 0, err
		}
		if peer.ID == n.cfg.NodeID {
			return 0, 0, status.Error(codes.Unavailable, "leader transition")
		}
		client, err := n.client(peer.GRPCAddr)
		if err != nil {
			return 0, 0, err
		}
		result, err := client.Head(ctx, &kvstorev1.HeadRequest{})
		if err != nil {
			return 0, 0, err
		}
		return result.GetRevision(), result.GetOldestAvailableRevision(), nil
	}
	if err := n.readStrong(ctx); err != nil {
		return 0, 0, err
	}
	return n.fsm.Head(ctx)
}

// Scan is served by the leader after verifying quorum and draining applies.
// It provides a consistent namespace snapshot/cursor for SDK cache resync.
func (n *Node) ScanNamespace(ctx context.Context, ns string, emit func(storage.ScanItem) error) error {
	if n.raft.State() != raft.Leader {
		peer, err := n.leaderPeer()
		if err != nil {
			return err
		}
		if peer.ID == n.cfg.NodeID {
			return status.Error(codes.Unavailable, "leader transition")
		}
		client, err := n.client(peer.GRPCAddr)
		if err != nil {
			return err
		}
		stream, err := client.Scan(ctx, &kvstorev1.ScanRequest{Namespace: ns})
		if err != nil {
			return err
		}
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := emit(storage.ScanItem{SnapshotRevision: msg.GetSnapshotRevision(), Key: msg.GetKey(), Value: msg.GetValue(), Revision: msg.GetRevision(), Checkpoint: msg.GetCheckpoint()}); err != nil {
				return err
			}
		}
	}
	if err := n.readStrong(ctx); err != nil {
		return err
	}
	return n.fsm.ScanNamespace(ctx, ns, emit)
}
