package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestClusterBenchmarkDefaults(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "cluster")
	t.Setenv("KVSTORE_NODE_ID", "node0")
	t.Setenv("KVSTORE_CLUSTER_BOOTSTRAP_NODE", "node0")
	t.Setenv("KVSTORE_RAFT_ADVERTISE_ADDR", "node0:7000")
	t.Setenv("KVSTORE_GRPC_ADVERTISE_ADDR", "node0:50051")
	t.Setenv("KVSTORE_ADMIN_ADVERTISE_ADDR", "node0:7001")
	t.Setenv("KVSTORE_JOIN_TOKEN", "test-only")
	for _, key := range []string{
		"KVSTORE_READ_BATCH_WINDOW", "KVSTORE_READ_BATCH_MAX", "KVSTORE_READ_VERIFY_PARALLELISM",
		"KVSTORE_WRITE_BATCH_WINDOW", "KVSTORE_WRITE_BATCH_MAX_WINDOW", "KVSTORE_WRITE_BATCH_TARGET",
		"KVSTORE_WRITE_BATCH_MAX", "KVSTORE_WRITE_MAX_QUEUE_DELAY", "KVSTORE_RAFT_MAX_INFLIGHT",
		"KVSTORE_RAFT_COMMIT_TIMEOUT", "KVSTORE_RAFT_MAX_APPEND_ENTRIES", "KVSTORE_RAFT_LOG_CACHE_ENTRIES",
	} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WriteBatchWindow != 1500*time.Microsecond || cfg.WriteBatchMaxWindow != 3*time.Millisecond ||
		cfg.WriteBatchMax != 256 || cfg.WriteBatchTarget != 64 || cfg.WriteMaxQueueDelay != 0 ||
		cfg.RaftMaxInflight != 8 || cfg.RaftCommitTimeout != 50*time.Millisecond ||
		cfg.RaftMaxAppendEntries != 128 || cfg.RaftLogCacheEntries != 512 ||
		cfg.ReadBatchWindow != 300*time.Microsecond || cfg.ReadBatchMax != 256 || cfg.ReadVerifyParallelism != 2 {
		t.Fatalf("unexpected cluster defaults: %+v", cfg)
	}
}

func TestStandaloneDefaults(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "standalone")
	t.Setenv("KVSTORE_EXPECTED_NODE_ID", "")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode != ModeStandalone || c.ListenAddr == "" {
		t.Fatalf("unexpected config %+v", c)
	}
}
func TestClusterConfiguration(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "cluster")
	t.Setenv("KVSTORE_RAFT_BOOTSTRAP_MODE", "legacy")
	t.Setenv("KVSTORE_NODE_ID", "kvstore-1")
	t.Setenv("KVSTORE_CLUSTER_BOOTSTRAP_NODE", "kvstore-0")
	t.Setenv("KVSTORE_RAFT_ADVERTISE_ADDR", "kvstore-1:7000")
	t.Setenv("KVSTORE_GRPC_ADVERTISE_ADDR", "kvstore-1:50051")
	t.Setenv("KVSTORE_ADMIN_ADVERTISE_ADDR", "kvstore-1:7001")
	t.Setenv("KVSTORE_CLUSTER_SEEDS", "kvstore-0:7001")
	t.Setenv("KVSTORE_JOIN_TOKEN", "secret")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Bootstrap || len(c.ClusterSeeds) != 1 {
		t.Fatalf("unexpected config %+v", c)
	}
}

func TestExplicitRaftBootstrapConfiguration(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "cluster")
	t.Setenv("KVSTORE_NODE_ID", "kvstore-0")
	t.Setenv("KVSTORE_CLUSTER_BOOTSTRAP_NODE", "kvstore-0")
	t.Setenv("KVSTORE_RAFT_ADVERTISE_ADDR", "kvstore-0:7000")
	t.Setenv("KVSTORE_GRPC_ADVERTISE_ADDR", "kvstore-0:50051")
	t.Setenv("KVSTORE_ADMIN_ADVERTISE_ADDR", "kvstore-0:7001")
	t.Setenv("KVSTORE_JOIN_TOKEN", "secret")
	t.Setenv("KVSTORE_CLUSTER_MIN_VOTERS", "3")
	t.Setenv("KVSTORE_CLUSTER_SEEDS", "kvstore-0:7001, kvstore-1:7001, kvstore-2:7001")
	t.Setenv("KVSTORE_RAFT_BOOTSTRAP_MODE", "explicit")

	cfg, err := Load()
	if err != nil || !cfg.Bootstrap || cfg.RaftBootstrapMode != RaftBootstrapExplicit || len(cfg.RaftBootstrapPeers()) != 3 {
		t.Fatalf("explicit bootstrap config %+v, error %v", cfg, err)
	}
	for _, tc := range []struct {
		key   string
		value string
	}{
		{"KVSTORE_RAFT_BOOTSTRAP_MODE", "automatic"},
		{"KVSTORE_RAFT_BOOTSTRAP_MODE", ""},
		{"KVSTORE_CLUSTER_SEEDS", ""},
		{"KVSTORE_CLUSTER_SEEDS", "kvstore-0:7001,kvstore-1:7001,kvstore-1:7001"},
		{"KVSTORE_CLUSTER_MIN_VOTERS", "2"},
		{"KVSTORE_CLUSTER_BOOTSTRAP_NODE", ""},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected %s error, got %v", tc.key, err)
			}
		})
	}
}
func TestRejectMissingJoinToken(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "cluster")
	t.Setenv("KVSTORE_RAFT_ADVERTISE_ADDR", "kvstore-0:7000")
	t.Setenv("KVSTORE_GRPC_ADVERTISE_ADDR", "kvstore-0:50051")
	t.Setenv("KVSTORE_ADMIN_ADVERTISE_ADDR", "kvstore-0:7001")
	t.Setenv("KVSTORE_JOIN_TOKEN", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected error")
	}
}

func TestReadBatchConfig(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "standalone")
	t.Setenv("KVSTORE_READ_BATCH_WINDOW", "750us")
	t.Setenv("KVSTORE_READ_BATCH_MAX", "64")
	cfg, err := Load()
	if err != nil || cfg.ReadBatchMax != 64 || cfg.ReadBatchWindow.String() != "750µs" {
		t.Fatalf("invalid batch settings cfg=%+v err=%v", cfg, err)
	}
	t.Setenv("KVSTORE_READ_BATCH_WINDOW", "100ms")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid batch window")
	}
}

func TestRaftInflightConfig(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "standalone")
	t.Setenv("KVSTORE_RAFT_MAX_INFLIGHT", "32")
	cfg, err := Load()
	if err != nil || cfg.RaftMaxInflight != 32 {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
	t.Setenv("KVSTORE_RAFT_MAX_INFLIGHT", "129")
	if _, err := Load(); err == nil {
		t.Fatal("accepted invalid inflight limit")
	}
}
