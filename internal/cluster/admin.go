package cluster

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

func (n *Node) StartAdmin() error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/cluster/join", n.handleJoin)
	mux.HandleFunc("POST /v1/cluster/snapshot", n.handleSnapshot)
	mux.HandleFunc("GET /v1/cluster/performance", n.handlePerformance)
	listener, err := net.Listen("tcp", n.cfg.AdminListenAddr)
	if err != nil {
		return err
	}
	n.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := n.admin.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("admin listener failed", "error", err)
		}
	}()
	return nil
}

func (n *Node) authorized(r *http.Request) bool {
	received := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return subtle.ConstantTimeCompare([]byte(received), []byte(n.cfg.JoinToken)) == 1
}
func (n *Node) handleJoin(w http.ResponseWriter, r *http.Request) {
	if !n.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if n.raft.State() != raft.Leader {
		n.forwardAdmin(w, r)
		return
	}
	var peer peerInfo
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&peer); err != nil {
		http.Error(w, "invalid peer JSON", http.StatusBadRequest)
		return
	}
	if peer.ID == "" || strings.ContainsAny(peer.ID, " \n\t/") || !validPeerAddr(peer.RaftAddr) || !validPeerAddr(peer.GRPCAddr) || !validPeerAddr(peer.AdminAddr) {
		http.Error(w, "invalid peer ID or addresses", http.StatusBadRequest)
		return
	}
	if err := n.addVoter(r.Context(), peer); err != nil {
		slog.Error("join failed", "id", peer.ID, "error", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("joined"))
}

func validPeerAddr(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	return err == nil && port != ""
}

func (n *Node) forwardAdmin(w http.ResponseWriter, r *http.Request) {
	leader, err := n.leaderPeer()
	if err != nil {
		http.Error(w, "leader not available", http.StatusServiceUnavailable)
		return
	}
	if leader.ID == n.cfg.NodeID {
		http.Error(w, "leader transition", http.StatusServiceUnavailable)
		return
	}
	target := "http://" + leader.AdminAddr + r.URL.RequestURI()
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, target, r.Body)
	if err != nil {
		http.Error(w, "proxy request failed", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Authorization", "Bearer "+n.cfg.JoinToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "leader unreachable", http.StatusServiceUnavailable)
		return
	}
	defer res.Body.Close()
	w.WriteHeader(res.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(res.Body, 16<<10))
}

func (n *Node) addVoter(ctx context.Context, peer peerInfo) error {
	if n.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	if err := n.ensureRegistered(ctx); err != nil {
		return fmt.Errorf("register bootstrap peer: %w", err)
	}
	current, exists, err := n.fsm.Peer(peer.ID)
	if err != nil {
		return err
	}
	// The replicated address map is written before membership changes so that
	// every follower can route SDK traffic when the new node becomes leader.
	if !exists || current != peer {
		if _, err := n.apply(ctx, command{Operation: opPeer, Peer: &peer}); err != nil {
			return err
		}
	}
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	for _, existing := range configFuture.Configuration().Servers {
		if string(existing.ID) == peer.ID && string(existing.Address) == peer.RaftAddr {
			return nil
		}
	}
	change := n.raft.AddVoter(raft.ServerID(peer.ID), raft.ServerAddress(peer.RaftAddr), 0, opTimeout)
	return change.Error()
}

func (n *Node) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if !n.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if n.raft.State() != raft.Leader {
		n.forwardAdmin(w, r)
		return
	}
	if err := n.raft.Snapshot().Error(); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// RegisterSelf runs until the bootstrapped leader has replicated its own
// routing metadata. Re-elections may occur, so failures are retried.
func (n *Node) RegisterSelf(ctx context.Context) {
	if !n.cfg.Bootstrap {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if n.raft.State() == raft.Leader {
			c, cancel := context.WithTimeout(ctx, opTimeout)
			err := n.ensureRegistered(c)
			cancel()
			if err == nil {
				return
			}
			slog.Debug("leader metadata registration pending", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Internal diagnostics are intentionally only available on the token-protected
// admin listener, not on the public SDK gRPC service.
func (n *Node) handlePerformance(w http.ResponseWriter, r *http.Request) {
	if !n.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		NodeID    string           `json:"node_id"`
		RaftState string           `json:"raft_state"`
		Reads     ReadPerformance  `json:"reads"`
		Writes    WritePerformance `json:"writes"`
	}{NodeID: n.cfg.NodeID, RaftState: n.RaftState(), Reads: n.ReadPerformance(), Writes: n.writes.Performance()})
}
