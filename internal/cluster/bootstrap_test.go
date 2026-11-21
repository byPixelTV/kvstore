package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/hashicorp/raft"
)

const bootstrapTestToken = "explicit-bootstrap-test-token"

func explicitClusterConfigs(t *testing.T) [3]config.Config {
	t.Helper()
	var configs [3]config.Config
	seeds := []string{freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)}
	for i := range configs {
		raftAddr := freeTCPAddr(t)
		configs[i] = config.Config{
			Mode: config.ModeCluster, NodeID: fmt.Sprintf("node%d", i), DataDir: t.TempDir(),
			Bootstrap: i == 0, BootstrapNode: "node0", RaftBootstrapMode: config.RaftBootstrapExplicit,
			RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr, GRPCAdvertiseAddr: freeTCPAddr(t),
			AdminListenAddr: seeds[i], AdminAdvertiseAddr: seeds[i], ClusterSeeds: seeds,
			JoinToken: bootstrapTestToken, MinRaftVoters: 3,
		}
	}
	return configs
}

func openBootstrapTestNode(t *testing.T, cfg config.Config) *Node {
	t.Helper()
	n, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.StartAdmin(); err != nil {
		_ = n.Close()
		t.Fatal(err)
	}
	return n
}

func bootstrapRequest(ctx context.Context, method, addr, auth string) (int, error) {
	call, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(call, method, "http://"+addr+raftBootstrapPath, nil)
	if err != nil {
		return 0, err
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	_ = res.Body.Close()
	return res.StatusCode, nil
}

func requireBootstrapStatus(t *testing.T, method, addr, auth string, want int) {
	t.Helper()
	if code, err := bootstrapRequest(t.Context(), method, addr, auth); err != nil || code != want {
		t.Fatalf("%s bootstrap at %s returned %d, want %d, error %v", method, addr, code, want, err)
	}
}

func TestExplicitRaftBootstrapActivation(t *testing.T) {
	configs := explicitClusterConfigs(t)
	node := openBootstrapTestNode(t, configs[0])
	t.Cleanup(func() { _ = node.Close() })
	addr := configs[0].AdminAdvertiseAddr
	auth := "Bearer " + bootstrapTestToken
	requireBootstrapStatus(t, http.MethodGet, addr, "", http.StatusUnauthorized)
	requireBootstrapStatus(t, http.MethodPost, addr, bootstrapTestToken, http.StatusUnauthorized)
	requireBootstrapStatus(t, http.MethodPost, addr, "Bearer wrong", http.StatusUnauthorized)
	requireBootstrapStatus(t, http.MethodPost, addr, auth, http.StatusServiceUnavailable)
	requireBootstrapStatus(t, http.MethodGet, addr, auth, http.StatusOK)
	if node.raft.LastIndex() != 0 || node.voterCount() != 0 || node.ClientTrafficReady() {
		t.Fatal("node initialized or admitted client traffic before activation")
	}

	peer2 := openBootstrapTestNode(t, configs[2])
	t.Cleanup(func() { _ = peer2.Close() })
	t.Run("peer plan mismatch", func(t *testing.T) {
		mismatchedCfg := configs[1]
		mismatchedCfg.BootstrapNode = "another-designation"
		mismatched := openBootstrapTestNode(t, mismatchedCfg)
		t.Cleanup(func() { _ = mismatched.Close() })
		requireBootstrapStatus(t, http.MethodPost, addr, auth, http.StatusServiceUnavailable)
	})
	peer1 := openBootstrapTestNode(t, configs[1])
	t.Cleanup(func() { _ = peer1.Close() })
	for _, cfg := range configs[1:] {
		requireBootstrapStatus(t, http.MethodPost, cfg.AdminAdvertiseAddr, auth, http.StatusForbidden)
	}

	type result struct {
		code int
		err  error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			code, err := bootstrapRequest(t.Context(), http.MethodPost, addr, auth)
			results <- result{code: code, err: err}
		}()
	}
	activations := 0
	for range 2 {
		r := <-results
		if r.err != nil || (r.code != http.StatusNoContent && r.code != http.StatusConflict) {
			t.Fatalf("unexpected bootstrap response: %+v", r)
		}
		if r.code == http.StatusNoContent {
			activations++
		}
	}
	if activations != 1 {
		t.Fatalf("got %d accepted bootstrap requests, want exactly one", activations)
	}
	waitLeader(t, node)
	requireBootstrapStatus(t, http.MethodPost, addr, auth, http.StatusConflict)
	requireBootstrapStatus(t, http.MethodGet, addr, auth, http.StatusConflict)
	node.RefreshAdmission()
	if node.ClientTrafficReady() {
		t.Fatal("one-voter group admitted client traffic")
	}
}

func TestExplicitRaftRejectsFSMWithoutWAL(t *testing.T) {
	cfg := explicitClusterConfigs(t)[0]
	fsm, err := openFSM(filepath.Join(cfg.DataDir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(newSetCommand("recovery", "stale", []byte("data"), 0))
	if err != nil {
		_ = fsm.Close()
		t.Fatal(err)
	}
	applied := fsm.Apply(&raft.Log{Index: 3, Type: raft.LogCommand, Data: raw}).(applyResult)
	if err := fsm.Close(); err != nil {
		t.Fatal(err)
	}
	if applied.Err != nil || applied.Revision != 1 {
		t.Fatalf("could not construct retained FSM data: %+v", applied)
	}
	n, err := Open(cfg)
	if n != nil {
		_ = n.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "FSM data exists without Raft state") {
		t.Fatalf("expected refusal to start with partial data, got %v", err)
	}
}
