package cluster

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/hashicorp/raft"
)

const raftBootstrapPath = "/v1/cluster/bootstrap"

type raftBootstrapStatus struct {
	NodeID        string   `json:"node_id"`
	RaftAddr      string   `json:"raft_addr"`
	AdminAddr     string   `json:"admin_addr"`
	BootstrapNode string   `json:"bootstrap_node"`
	MinVoters     int      `json:"min_voters"`
	Peers         []string `json:"peers"`
}

func bootstrapRaft(r *raft.Raft, cfg config.Config) error {
	return r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
		ID: raft.ServerID(cfg.NodeID), Address: raft.ServerAddress(cfg.RaftAdvertiseAddr), Suffrage: raft.Voter,
	}}}).Error()
}

func (n *Node) bootstrapAuthorized(r *http.Request) bool {
	auth := r.Header.Values("Authorization")
	return n.cfg.JoinToken != "" && len(auth) == 1 &&
		subtle.ConstantTimeCompare([]byte(auth[0]), []byte("Bearer "+n.cfg.JoinToken)) == 1
}

func (n *Node) bootstrapStateError() error {
	exists, err := raft.HasExistingState(n.raftStore, n.raftStore, n.snapshots)
	if err != nil {
		return err
	}
	if exists {
		return raft.ErrCantBootstrap
	}
	// A missing Raft directory with an intact FSM is a partial data loss.
	// Its applied index would cause an unrelated new log to skip entries.
	n.fsm.mu.RLock()
	defer n.fsm.mu.RUnlock()
	if n.fsm.failed != nil {
		return n.fsm.failed
	}
	if n.fsm.applied != 0 || n.fsm.revision != 0 {
		return raft.ErrCantBootstrap
	}
	return nil
}

func (n *Node) handleBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !n.bootstrapAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err := n.bootstrapStateError(); err != nil {
		writeBootstrapError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(raftBootstrapStatus{
		NodeID: n.cfg.NodeID, RaftAddr: n.cfg.RaftAdvertiseAddr, AdminAddr: n.cfg.AdminAdvertiseAddr,
		BootstrapNode: n.cfg.BootstrapNode, MinVoters: n.cfg.MinRaftVoters, Peers: n.cfg.RaftBootstrapPeers(),
	})
}

func (n *Node) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	if !n.bootstrapAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if n.cfg.NodeID != n.cfg.BootstrapNode {
		http.Error(w, "bootstrap requires the designated node", http.StatusForbidden)
		return
	}
	if err := n.bootstrapStateError(); err != nil {
		writeBootstrapError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	if err := n.verifyBootstrapPeers(ctx); err != nil {
		writeBootstrapError(w, err)
		return
	}
	if err := ctx.Err(); err != nil {
		writeBootstrapError(w, err)
		return
	}
	// Raft serializes the bootstrap request with its state machine and
	// rechecks durable Raft state before accepting it.
	if err := bootstrapRaft(n.raft, n.cfg); err != nil {
		writeBootstrapError(w, err)
		return
	}
	slog.Info("raft cluster initialized by operator", "node", n.cfg.NodeID)
	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) verifyBootstrapPeers(ctx context.Context) error {
	peers := n.cfg.RaftBootstrapPeers()
	seenIDs := map[string]bool{n.cfg.NodeID: true}
	seenRaftAddrs := map[string]bool{n.cfg.RaftAdvertiseAddr: true}
	client := http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	for _, seed := range peers {
		if seed == n.cfg.AdminAdvertiseAddr {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+seed+raftBootstrapPath, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+n.cfg.JoinToken)
		req.Header.Set("Cache-Control", "no-cache")
		res, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("check bootstrap peer %s: %w", seed, err)
		}
		if res.StatusCode != http.StatusOK {
			_ = res.Body.Close()
			if res.StatusCode == http.StatusConflict {
				return fmt.Errorf("%s: %w", seed, raft.ErrCantBootstrap)
			}
			return fmt.Errorf("check bootstrap peer %s: %s", seed, res.Status)
		}
		var remote raftBootstrapStatus
		err = json.NewDecoder(io.LimitReader(res.Body, 16<<10)).Decode(&remote)
		_ = res.Body.Close()
		if err != nil || remote.NodeID == "" || seenIDs[remote.NodeID] ||
			!validPeerAddr(remote.RaftAddr) || seenRaftAddrs[remote.RaftAddr] || remote.AdminAddr != seed ||
			remote.BootstrapNode != n.cfg.BootstrapNode || remote.MinVoters != n.cfg.MinRaftVoters ||
			!slices.Equal(remote.Peers, peers) {
			return fmt.Errorf("bootstrap peer %s does not match the initial cluster plan", seed)
		}
		seenIDs[remote.NodeID] = true
		seenRaftAddrs[remote.RaftAddr] = true
	}
	return nil
}

func writeBootstrapError(w http.ResponseWriter, err error) {
	if errors.Is(err, raft.ErrCantBootstrap) {
		http.Error(w, "cluster state already exists", http.StatusConflict)
		return
	}
	slog.Warn("raft bootstrap rejected", "error", err)
	http.Error(w, "could not verify or initialize the empty Raft cluster", http.StatusServiceUnavailable)
}
