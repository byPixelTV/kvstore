package cluster

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/byPixelTV/kvstore/internal/config"
	"github.com/byPixelTV/kvstore/internal/storage"
)

// TestRaftWALCrashRecoveryHelper is run only in a child process. The parent
// kills that process without calling Node.Close() to exercise WAL recovery
// after a real abrupt process termination (as opposed to graceful shutdown).
func TestRaftWALCrashRecoveryHelper(t *testing.T) {
	if os.Getenv("KVSTORE_WAL_CRASH_CHILD") != "1" {
		t.Skip("subprocess helper")
	}
	cfg := config.Config{
		Mode:               config.ModeCluster,
		NodeID:             "node0",
		DataDir:            os.Getenv("KVSTORE_WAL_CRASH_DIR"),
		Bootstrap:          true,
		RaftBindAddr:       os.Getenv("KVSTORE_WAL_CRASH_RAFT_ADDR"),
		RaftAdvertiseAddr:  os.Getenv("KVSTORE_WAL_CRASH_RAFT_ADDR"),
		GRPCAdvertiseAddr:  os.Getenv("KVSTORE_WAL_CRASH_GRPC_ADDR"),
		AdminListenAddr:    os.Getenv("KVSTORE_WAL_CRASH_ADMIN_ADDR"),
		AdminAdvertiseAddr: os.Getenv("KVSTORE_WAL_CRASH_ADMIN_ADDR"),
		JoinToken:          "test-only",
	}
	n, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	operations := make([]storage.WriteOperation, 8)
	for i := range operations {
		operations[i] = storage.WriteOperation{Namespace: "crash", Key: fmt.Sprintf("key-%d", i), Value: []byte(fmt.Sprintf("value-%d", i))}
	}
	revisions, err := n.BatchWrite(ctx, operations)
	if err != nil || len(revisions) != len(operations) {
		t.Fatalf("acknowledge writes: revisions=%v err=%v", revisions, err)
	}
	fmt.Fprintln(os.Stdout, "WAL_CRASH_READY")
	// Intentionally no Close, Shutdown or fsync in the test harness.
	for {
		time.Sleep(time.Hour)
	}
}

func TestRaftWALRecoversAcknowledgedWritesAfterSIGKILL(t *testing.T) {
	if os.Getenv("KVSTORE_WAL_CRASH_CHILD") == "1" {
		t.Skip("parent only")
	}
	dir := t.TempDir()
	raftAddr, grpcAddr, adminAddr := freeTCPAddr(t), freeTCPAddr(t), freeTCPAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRaftWALCrashRecoveryHelper$", "-test.v")
	cmd.Env = append(os.Environ(),
		"KVSTORE_WAL_CRASH_CHILD=1",
		"KVSTORE_WAL_CRASH_DIR="+dir,
		"KVSTORE_WAL_CRASH_RAFT_ADDR="+raftAddr,
		"KVSTORE_WAL_CRASH_GRPC_ADDR="+grpcAddr,
		"KVSTORE_WAL_CRASH_ADMIN_ADDR="+adminAddr,
	)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(pipe)
	ready := false
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "WAL_CRASH_READY") {
			ready = true
			break
		}
	}
	if !ready {
		_ = cmd.Wait()
		t.Fatalf("child did not confirm acknowledged writes: scan=%v stderr=%s", scanner.Err(), stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	cfg := config.Config{
		Mode: config.ModeCluster, NodeID: "node0", DataDir: dir, Bootstrap: true,
		RaftBindAddr: raftAddr, RaftAdvertiseAddr: raftAddr,
		GRPCAdvertiseAddr: grpcAddr, AdminListenAddr: adminAddr, AdminAdvertiseAddr: adminAddr,
		JoinToken: "test-only",
	}
	n, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen after SIGKILL: %v", err)
	}
	defer n.Close()
	waitLeader(t, n)
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("key-%d", i)
		rec, found, err := n.Get(ctx, "crash", key)
		if err != nil || !found || string(rec.Value) != fmt.Sprintf("value-%d", i) || rec.Revision != uint64(i+1) {
			t.Fatalf("lost previously acknowledged write %s after kill: found=%v rec=%+v err=%v", key, found, rec, err)
		}
	}
	changes, err := n.ReadChanges(ctx, 0, 16)
	if err != nil || len(changes.Events) != 8 {
		t.Fatalf("changefeed after SIGKILL: entries=%d err=%v", len(changes.Events), err)
	}
}
