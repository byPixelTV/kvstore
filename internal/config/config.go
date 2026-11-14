package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ModeStandalone = "standalone"
	ModeCluster    = "cluster"
)

type Config struct {
	Mode                  string
	NodeID                string
	ListenAddr            string
	DataDir               string
	ShutdownTimeout       time.Duration
	RaftBindAddr          string
	RaftAdvertiseAddr     string
	GRPCAdvertiseAddr     string
	AdminListenAddr       string
	AdminAdvertiseAddr    string
	ClusterSeeds          []string
	Bootstrap             bool
	BootstrapNode         string
	JoinToken             string
	ReadBatchWindow       time.Duration
	ReadBatchMax          int
	ReadVerifyParallelism int
	WriteBatchWindow      time.Duration
	WriteBatchMax         int
	WriteBatchTarget      int
	WriteBatchMaxWindow   time.Duration
	RaftMaxAppendEntries  int
	RaftCommitTimeout     time.Duration
	RaftMaxInflight       int
}

func getenv(key, def string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return def
}

func validAddr(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("invalid numeric port: %q", port)
	}
	return nil
}

func Load() (Config, error) {
	c := Config{
		Mode:               getenv("KVSTORE_MODE", ModeStandalone),
		NodeID:             getenv("KVSTORE_NODE_ID", "node-1"),
		ListenAddr:         getenv("KVSTORE_LISTEN_ADDR", "127.0.0.1:50051"),
		DataDir:            getenv("KVSTORE_DATA_DIR", "./data"),
		RaftBindAddr:       getenv("KVSTORE_RAFT_BIND_ADDR", ":7000"),
		RaftAdvertiseAddr:  getenv("KVSTORE_RAFT_ADVERTISE_ADDR", ""),
		GRPCAdvertiseAddr:  getenv("KVSTORE_GRPC_ADVERTISE_ADDR", ""),
		AdminListenAddr:    getenv("KVSTORE_ADMIN_LISTEN_ADDR", ":7001"),
		AdminAdvertiseAddr: getenv("KVSTORE_ADMIN_ADVERTISE_ADDR", ""),
		BootstrapNode:      getenv("KVSTORE_CLUSTER_BOOTSTRAP_NODE", "node-1"),
		JoinToken:          os.Getenv("KVSTORE_JOIN_TOKEN"),
	}
	if c.Mode != ModeStandalone && c.Mode != ModeCluster {
		return c, fmt.Errorf("invalid KVSTORE_MODE %q", c.Mode)
	}
	if c.NodeID == "" || strings.ContainsAny(c.NodeID, " \t\n/") {
		return c, fmt.Errorf("invalid KVSTORE_NODE_ID %q", c.NodeID)
	}
	if c.DataDir == "" {
		return c, fmt.Errorf("KVSTORE_DATA_DIR cannot be empty")
	}
	if err := validAddr(c.ListenAddr); err != nil {
		return c, fmt.Errorf("KVSTORE_LISTEN_ADDR: %w", err)
	}
	var err error
	c.ShutdownTimeout, err = time.ParseDuration(getenv("KVSTORE_SHUTDOWN_TIMEOUT", "15s"))
	if err != nil || c.ShutdownTimeout <= 0 {
		return c, fmt.Errorf("KVSTORE_SHUTDOWN_TIMEOUT must be positive")
	}
	// Read fencing batches only requests queued before a new quorum check.
	c.ReadBatchWindow, err = time.ParseDuration(getenv("KVSTORE_READ_BATCH_WINDOW", "300us"))
	if err != nil || c.ReadBatchWindow <= 0 || c.ReadBatchWindow > 20*time.Millisecond {
		return c, fmt.Errorf("KVSTORE_READ_BATCH_WINDOW must be greater than 0s and at most 20ms")
	}
	c.ReadBatchMax, err = strconv.Atoi(getenv("KVSTORE_READ_BATCH_MAX", "256"))
	if err != nil || c.ReadBatchMax < 1 || c.ReadBatchMax > 4096 {
		return c, fmt.Errorf("KVSTORE_READ_BATCH_MAX must be between 1 and 4096")
	}
	c.ReadVerifyParallelism, err = strconv.Atoi(getenv("KVSTORE_READ_VERIFY_PARALLELISM", "2"))
	if err != nil || c.ReadVerifyParallelism < 1 || c.ReadVerifyParallelism > 64 {
		return c, fmt.Errorf("KVSTORE_READ_VERIFY_PARALLELISM must be between 1 and 64")
	}
	c.WriteBatchWindow, err = time.ParseDuration(getenv("KVSTORE_WRITE_BATCH_WINDOW", "250us"))
	if err != nil || c.WriteBatchWindow <= 0 || c.WriteBatchWindow > 20*time.Millisecond {
		return c, fmt.Errorf("KVSTORE_WRITE_BATCH_WINDOW must be greater than 0s and at most 20ms")
	}
	c.WriteBatchMax, err = strconv.Atoi(getenv("KVSTORE_WRITE_BATCH_MAX", "128"))
	if err != nil || c.WriteBatchMax < 1 || c.WriteBatchMax > 4096 {
		return c, fmt.Errorf("KVSTORE_WRITE_BATCH_MAX must be between 1 and 4096")
	}
	c.WriteBatchTarget, err = strconv.Atoi(getenv("KVSTORE_WRITE_BATCH_TARGET", "24"))
	if err != nil || c.WriteBatchTarget < 1 || c.WriteBatchTarget > 256 {
		return c, fmt.Errorf("KVSTORE_WRITE_BATCH_TARGET must be between 1 and 256")
	}
	maxWindowText := os.Getenv("KVSTORE_WRITE_BATCH_MAX_WINDOW")
	if maxWindowText == "" {
		maxWindowText = "5ms"
	}
	c.WriteBatchMaxWindow, err = time.ParseDuration(maxWindowText)
	if os.Getenv("KVSTORE_WRITE_BATCH_MAX_WINDOW") == "" && c.WriteBatchMaxWindow < c.WriteBatchWindow {
		c.WriteBatchMaxWindow = c.WriteBatchWindow
	}
	if err != nil || c.WriteBatchMaxWindow < c.WriteBatchWindow || c.WriteBatchMaxWindow > 20*time.Millisecond {
		return c, fmt.Errorf("KVSTORE_WRITE_BATCH_MAX_WINDOW must be >= KVSTORE_WRITE_BATCH_WINDOW and <= 20ms")
	}
	c.RaftMaxAppendEntries, err = strconv.Atoi(getenv("KVSTORE_RAFT_MAX_APPEND_ENTRIES", "128"))
	if err != nil || c.RaftMaxAppendEntries < 1 || c.RaftMaxAppendEntries > 1024 {
		return c, fmt.Errorf("KVSTORE_RAFT_MAX_APPEND_ENTRIES must be between 1 and 1024")
	}
	c.RaftCommitTimeout, err = time.ParseDuration(getenv("KVSTORE_RAFT_COMMIT_TIMEOUT", "50ms"))
	if err != nil || c.RaftCommitTimeout < time.Millisecond || c.RaftCommitTimeout > time.Second {
		return c, fmt.Errorf("KVSTORE_RAFT_COMMIT_TIMEOUT must be between 1ms and 1s")
	}
	c.RaftMaxInflight, err = strconv.Atoi(getenv("KVSTORE_RAFT_MAX_INFLIGHT", "16"))
	if err != nil || c.RaftMaxInflight < 1 || c.RaftMaxInflight > 128 {
		return c, fmt.Errorf("KVSTORE_RAFT_MAX_INFLIGHT must be between 1 and 128")
	}
	if c.Mode == ModeStandalone {
		if expected := os.Getenv("KVSTORE_EXPECTED_NODE_ID"); expected != "" && expected != c.NodeID {
			return c, fmt.Errorf("standalone instance %q is not the permitted singleton %q", c.NodeID, expected)
		}
		return c, nil
	}
	c.Bootstrap = c.NodeID == c.BootstrapNode
	for _, v := range strings.Split(os.Getenv("KVSTORE_CLUSTER_SEEDS"), ",") {
		if v = strings.TrimSpace(v); v != "" {
			c.ClusterSeeds = append(c.ClusterSeeds, v)
		}
	}
	for name, addr := range map[string]string{
		"KVSTORE_RAFT_BIND_ADDR":       c.RaftBindAddr,
		"KVSTORE_RAFT_ADVERTISE_ADDR":  c.RaftAdvertiseAddr,
		"KVSTORE_GRPC_ADVERTISE_ADDR":  c.GRPCAdvertiseAddr,
		"KVSTORE_ADMIN_LISTEN_ADDR":    c.AdminListenAddr,
		"KVSTORE_ADMIN_ADVERTISE_ADDR": c.AdminAdvertiseAddr,
	} {
		if err := validAddr(addr); err != nil {
			return c, fmt.Errorf("%s: %w", name, err)
		}
	}
	for _, seed := range c.ClusterSeeds {
		if err := validAddr(seed); err != nil {
			return c, fmt.Errorf("KVSTORE_CLUSTER_SEEDS %q: %w", seed, err)
		}
	}
	if c.JoinToken == "" {
		return c, fmt.Errorf("KVSTORE_JOIN_TOKEN must be set in cluster mode")
	}
	if !c.Bootstrap && len(c.ClusterSeeds) == 0 {
		return c, fmt.Errorf("KVSTORE_CLUSTER_SEEDS required for joining nodes")
	}
	return c, nil
}
