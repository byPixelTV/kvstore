package config

import "testing"

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
