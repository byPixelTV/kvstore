package config

import (
	"testing"
	"time"
)

func TestRaftIOTuningDefaults(t *testing.T) {
	t.Setenv("KVSTORE_MODE", ModeStandalone)
	// Config uses the explicitly provided values. Set realistic defaults
	// directly to avoid depending on the parent process environment.
	t.Setenv("KVSTORE_READ_VERIFY_PARALLELISM", "2")
	t.Setenv("KVSTORE_WRITE_BATCH_TARGET", "24")
	t.Setenv("KVSTORE_WRITE_BATCH_MAX_WINDOW", "5ms")
	t.Setenv("KVSTORE_RAFT_MAX_APPEND_ENTRIES", "128")
	t.Setenv("KVSTORE_RAFT_COMMIT_TIMEOUT", "50ms")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReadVerifyParallelism != 2 || cfg.WriteBatchTarget != 24 || cfg.WriteBatchMaxWindow != 5*time.Millisecond || cfg.RaftMaxAppendEntries != 128 || cfg.RaftCommitTimeout != 50*time.Millisecond {
		t.Fatalf("bad raft I/O tuning %+v", cfg)
	}
}

func TestRejectInvalidRaftIOTuning(t *testing.T) {
	for _, pair := range [][2]string{
		{"KVSTORE_READ_VERIFY_PARALLELISM", "0"},
		{"KVSTORE_READ_VERIFY_PARALLELISM", "65"},
		{"KVSTORE_WRITE_BATCH_TARGET", "0"},
		{"KVSTORE_WRITE_BATCH_TARGET", "257"},
		{"KVSTORE_WRITE_BATCH_MAX_WINDOW", "1us"},
		{"KVSTORE_WRITE_BATCH_MAX_WINDOW", "50ms"},
		{"KVSTORE_RAFT_MAX_APPEND_ENTRIES", "1025"},
		{"KVSTORE_RAFT_COMMIT_TIMEOUT", "0s"},
		{"KVSTORE_RAFT_COMMIT_TIMEOUT", "2s"},
	} {
		t.Run(pair[0]+"="+pair[1], func(t *testing.T) {
			t.Setenv("KVSTORE_MODE", ModeStandalone)
			t.Setenv(pair[0], pair[1])
			if _, err := Load(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
