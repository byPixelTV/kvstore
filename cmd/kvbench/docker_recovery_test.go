//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/sharding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const recoveryNamespace = "docker-recovery"
const recoveryJoinToken = "isolated-recovery-test"

type recoveryCompose struct {
	root string
	args []string
}

func (c recoveryCompose) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", append(append([]string(nil), c.args...), args...)...)
	cmd.Dir = c.root
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker compose %s: %w\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output)), nil
}

func startRecoveryCompose(t *testing.T, ctx context.Context) recoveryCompose {
	t.Helper()
	image := os.Getenv("KVSTORE_TEST_IMAGE")
	if image == "" {
		t.Skip("set KVSTORE_TEST_IMAGE to a locally built KVStore image to run destructive tests against a fresh Compose project")
	}
	t.Setenv("KVSTORE_IMAGE", image)
	t.Setenv("KVSTORE_JOIN_TOKEN", recoveryJoinToken)
	t.Setenv("KVSTORE_ROUTER_ADMIN_TOKEN", recoveryJoinToken+"-router")

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	override := filepath.Join(t.TempDir(), "compose.yml")
	// Publish only random local ports. The test controls every restart.
	var yaml strings.Builder
	yaml.WriteString("services:\n  router:\n    restart: 'no'\n    ports: !override ['127.0.0.1::50051']\n")
	for s := range sharding.ShardCount {
		for n := range 3 {
			fmt.Fprintf(&yaml, "  s%dn%d:\n    restart: 'no'\n    ports: !override ['127.0.0.1::7001', '127.0.0.1::50051']\n    environment:\n      KVSTORE_RAFT_BOOTSTRAP_MODE: explicit\n", s, n)
		}
	}
	yaml.WriteString("volumes:\n  s0n0-replacement-data:\n")
	if err := os.WriteFile(override, []byte(yaml.String()), 0600); err != nil {
		t.Fatal(err)
	}
	project := fmt.Sprintf("kvstore-recovery-%d-%d", os.Getpid(), time.Now().Unix())
	c := recoveryCompose{root: root, args: []string{"compose", "-p", project, "-f", "compose.sharded.yml", "-f", override}}
	t.Logf("isolated Compose project %s (%s)", project, image)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if t.Failed() {
			logs, logErr := c.run(cleanup, "logs", "--tail", "12")
			t.Logf("Compose log tail:\n%s\n%v", logs, logErr)
		}
		if _, err := c.run(cleanup, "down", "--volumes", "--timeout", "3"); err != nil {
			t.Error(err)
		}
	})
	recoveryDocker(t, ctx, c, "up", "-d", "--no-build", "--pull", "never")
	for s := range sharding.ShardCount {
		var admins [3]string
		for n := range admins {
			admins[n] = recoveryDocker(t, ctx, c, "port", fmt.Sprintf("s%dn%d", s, n), "7001")
		}
		waitRecovery(t, ctx, fmt.Sprintf("all shard %d admin endpoints eligible for bootstrap", s), func() bool {
			for _, addr := range admins {
				if code, _ := recoveryBootstrap(ctx, addr, http.MethodGet); code != http.StatusOK {
					return false
				}
			}
			return true
		})
		// compose.sharded.yml designates node s for shard s.
		waitRecovery(t, ctx, fmt.Sprintf("shard %d bootstrap", s), func() bool {
			code, err := recoveryBootstrap(ctx, admins[s], http.MethodPost)
			if err == nil && code != http.StatusNoContent && code != http.StatusServiceUnavailable {
				t.Fatalf("bootstrap shard %d: status=%d", s, code)
			}
			return err == nil && code == http.StatusNoContent
		})
	}
	return c
}

func recoveryDocker(t *testing.T, ctx context.Context, c recoveryCompose, args ...string) string {
	t.Helper()
	output, err := c.run(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func recoveryClient(t *testing.T, ctx context.Context, c recoveryCompose, service string) kvstorev1.KVStoreClient {
	t.Helper()
	addr := recoveryDocker(t, ctx, c, "port", service, "50051")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return kvstorev1.NewKVStoreClient(conn)
}

func recoveryBootstrap(ctx context.Context, addr, method string) (int, error) {
	call, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(call, method, "http://"+addr+"/v1/cluster/bootstrap", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+recoveryJoinToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 16<<10))
	return res.StatusCode, nil
}

type recoveryReplica struct {
	Voters int    `json:"voters"`
	State  string `json:"raft_state"`
}

func recoveryStatus(ctx context.Context, addr string) recoveryReplica {
	call, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(call, http.MethodGet, "http://"+addr+"/v1/cluster/performance", nil)
	if err != nil {
		return recoveryReplica{}
	}
	req.Header.Set("Authorization", "Bearer "+recoveryJoinToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return recoveryReplica{}
	}
	defer res.Body.Close()
	var metrics recoveryReplica
	if res.StatusCode == http.StatusOK && json.NewDecoder(res.Body).Decode(&metrics) == nil {
		return metrics
	}
	return recoveryReplica{}
}

func waitRecovery(t *testing.T, ctx context.Context, what string, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(45 * time.Second)
	defer timeout.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-timeout.C:
			t.Fatalf("timed out waiting for %s", what)
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitRecoveryRouter(t *testing.T, ctx context.Context, router kvstorev1.KVStoreClient) {
	t.Helper()
	waitRecovery(t, ctx, "STRONG Head on all shards", func() bool {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := router.Head(probe, &kvstorev1.HeadRequest{UseVectorCursor: true})
		return err == nil
	})
}

const (
	beforeLeaderCrash = iota
	afterLeaderCrash
	withoutQuorum
	afterQuorumRestore
	recoveryPhases
)

type recoveryLoad struct {
	namespace string
	keyPrefix string
	phaseMu   sync.RWMutex
	phase     int
	mu        sync.Mutex
	attempts  [recoveryPhases][sharding.ShardCount]int
	acks      [recoveryPhases][sharding.ShardCount]int
	lastAck   [sharding.ShardCount]time.Time
	maxAckGap [sharding.ShardCount]time.Duration
	journal   map[string]uint64 // Values equal their unique keys.
	err       error
}

func recoveryTransient(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Canceled
}

func (l *recoveryLoad) run(ctx context.Context, client kvstorev1.KVStoreClient, worker int) {
	const batchSize = 16
	shard := worker % sharding.ShardCount
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		l.phaseMu.RLock()
		phase := l.phase
		ops := make([]*kvstorev1.BatchWriteOperation, 0, batchSize)
		for len(ops) < batchSize {
			seq++
			key := fmt.Sprintf("%s%d/%d", l.keyPrefix, worker, seq)
			if sharding.Shard(l.namespace, key) == shard {
				ops = append(ops, &kvstorev1.BatchWriteOperation{Namespace: l.namespace, Key: key, Value: []byte(key)})
			}
		}
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		reply, err := client.BatchWrite(call, &kvstorev1.BatchWriteRequest{Operations: ops})
		cancel()

		l.mu.Lock()
		l.attempts[phase][shard] += len(ops)
		switch {
		case err != nil:
			if !recoveryTransient(status.Code(err)) && l.err == nil {
				l.err = err
			}
		case len(reply.GetResults()) != len(ops):
			l.err = fmt.Errorf("unexpected BatchWrite result count: %d", len(reply.GetResults()))
		default:
			acknowledged := false
			for i, item := range reply.GetResults() {
				code := codes.Code(item.GetGrpcCode())
				if code == codes.OK && item.GetRevision() > 0 {
					l.acks[phase][shard]++
					l.journal[ops[i].Key] = item.GetRevision()
					acknowledged = true
				} else if !recoveryTransient(code) && l.err == nil {
					l.err = fmt.Errorf("invalid result for %s: %v", ops[i].Key, item)
				}
			}
			if acknowledged {
				now := time.Now()
				if last := l.lastAck[shard]; !last.IsZero() {
					l.maxAckGap[shard] = max(l.maxAckGap[shard], now.Sub(last))
				}
				l.lastAck[shard] = now
			}
		}
		l.mu.Unlock()
		l.phaseMu.RUnlock()
	}
}

func (l *recoveryLoad) setPhase(phase int) {
	l.phaseMu.Lock()
	l.phase = phase
	l.phaseMu.Unlock()
}

func (l *recoveryLoad) counts(t *testing.T, phase int) (attempts, acks [sharding.ShardCount]int) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		t.Fatal(l.err)
	}
	return l.attempts[phase], l.acks[phase]
}

func TestDockerClusterRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	c := startRecoveryCompose(t, ctx)
	router := recoveryClient(t, ctx, c, "router")
	waitRecoveryRouter(t, ctx, router)
	affected := [3]string{"s0n0", "s0n1", "s0n2"}
	var admin [3]string
	for i, service := range affected {
		admin[i] = recoveryDocker(t, ctx, c, "port", service, "7001")
	}
	waitRecovery(t, ctx, "three voters on all replicas of shard 0", func() bool {
		for _, addr := range admin {
			if recoveryStatus(ctx, addr).Voters != len(affected) {
				return false
			}
		}
		return true
	})

	key := "sentinel"
	for sharding.Shard(recoveryNamespace, key) != 0 {
		key += "x"
	}
	sentinel, err := router.Set(ctx, &kvstorev1.SetRequest{Namespace: recoveryNamespace, Key: key, Value: []byte(key)})
	if err != nil || sentinel.GetRevision() == 0 {
		t.Fatalf("write sentinel: %v %v", sentinel, err)
	}
	load := &recoveryLoad{namespace: recoveryNamespace, journal: map[string]uint64{key: sentinel.GetRevision()}}
	var workers sync.WaitGroup
	workCtx, stopWork := context.WithCancel(ctx)
	defer func() { stopWork(); workers.Wait() }()
	for i := range 12 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			load.run(workCtx, router, i)
		}()
	}
	waitRecovery(t, ctx, "live durable writes", func() bool {
		_, acks := load.counts(t, beforeLeaderCrash)
		return acks[0] >= 256 && acks[1] >= 256 && acks[2] >= 256
	})
	findLeader := func(excluded int) int {
		for i, addr := range admin {
			if i != excluded && recoveryStatus(ctx, addr).State == "Leader" {
				return i
			}
		}
		return -1
	}
	leader := findLeader(-1)
	if leader < 0 {
		t.Fatal("shard 0 has no leader before fault")
	}
	failoverStart := time.Now()
	recoveryDocker(t, ctx, c, "kill", "-s", "SIGKILL", affected[leader])
	load.setPhase(afterLeaderCrash)
	waitRecovery(t, ctx, "acknowledged writes after leader crash", func() bool {
		_, acks := load.counts(t, afterLeaderCrash)
		return acks[0] >= 256
	})
	newLeader := findLeader(leader)
	if newLeader < 0 {
		t.Fatal("surviving leader not found")
	}
	t.Logf("leader failed over from %s to %s under load in %s", affected[leader], affected[newLeader], time.Since(failoverStart).Round(time.Millisecond))

	follower := 3 - leader - newLeader
	newLeaderClient := recoveryClient(t, ctx, c, affected[newLeader])
	probe := &kvstorev1.GetRequest{Namespace: recoveryNamespace, Key: key}
	strongCtx, strongCancel := context.WithTimeout(ctx, 5*time.Second)
	value, err := newLeaderClient.Get(strongCtx, probe)
	strongCancel()
	if err != nil || !value.GetFound() || string(value.GetValue()) != key {
		t.Fatalf("STRONG read on new leader with quorum: %v %v", value, err)
	}
	recoveryDocker(t, ctx, c, "kill", "-s", "SIGKILL", affected[follower])
	load.setPhase(withoutQuorum)
	strongCtx, strongCancel = context.WithTimeout(ctx, 5*time.Second)
	_, err = newLeaderClient.Get(strongCtx, probe)
	strongCancel()
	if !recoveryTransient(status.Code(err)) {
		t.Fatalf("STRONG read on surviving replica after quorum loss: %v", err)
	}
	waitRecovery(t, ctx, "write pressure during quorum loss", func() bool {
		attempts, acks := load.counts(t, withoutQuorum)
		if acks[0] != 0 {
			t.Fatalf("shard acknowledged %d writes without quorum", acks[0])
		}
		return attempts[0] >= 512 && acks[1] >= 512 && acks[2] >= 512
	})
	// Wait for every outage-phase request before restoring quorum.
	load.setPhase(afterQuorumRestore)
	attempts, acks := load.counts(t, withoutQuorum)
	if acks[0] != 0 {
		t.Fatalf("shard acknowledged %d writes without quorum", acks[0])
	}
	t.Logf("leader failover and quorum loss verified: attempts=%v acknowledgements=%v", attempts, acks)

	recoveryDocker(t, ctx, c, "start", affected[leader], affected[follower])
	// Docker can assign a different published port when a stopped container
	// rejoins the test network.
	for _, restarted := range []int{leader, follower} {
		admin[restarted] = recoveryDocker(t, ctx, c, "port", affected[restarted], "7001")
	}
	waitRecovery(t, ctx, "acknowledged writes after quorum restore", func() bool {
		_, acks := load.counts(t, afterQuorumRestore)
		return acks[0] >= 256
	})

	// Recreate the original bootstrap voter with a fresh project-scoped volume.
	// The other two voters retain their storage and the original volume is
	// left alone until the isolated project is cleaned up.
	recoveryDocker(t, ctx, c, "stop", "-t", "3", affected[0])
	_, beforeReplacement := load.counts(t, afterQuorumRestore)
	waitRecovery(t, ctx, "surviving quorum before empty volume replacement", func() bool {
		_, acks := load.counts(t, afterQuorumRestore)
		return acks[0] >= beforeReplacement[0]+256
	})
	override := filepath.Join(t.TempDir(), "replacement.yml")
	replacementYAML := []byte("services:\n  s0n0:\n    volumes:\n      - s0n0-replacement-data:/data\n")
	if err := os.WriteFile(override, replacementYAML, 0600); err != nil {
		t.Fatal(err)
	}
	c.args = append(append([]string(nil), c.args...), "-f", override)
	recoveryDocker(t, ctx, c, "up", "-d", "--no-build", "--pull", "never", "--no-deps", "--force-recreate", affected[0])
	replacement := recoveryClient(t, ctx, c, affected[0])
	var replacementProbe string
	defer func() {
		if t.Failed() {
			t.Logf("empty replacement probe: %s", replacementProbe)
		}
	}()
	waitRecovery(t, ctx, "original revision restored locally on the empty replacement", func() bool {
		call, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		value, err := replacement.Get(call, &kvstorev1.GetRequest{
			Namespace: recoveryNamespace, Key: key, Consistency: kvstorev1.ReadConsistency_READ_CONSISTENCY_EVENTUAL,
		})
		encoded, encodeErr := sharding.EncodeRevision(0, value.GetRevision())
		replacementProbe = fmt.Sprintf("found=%t encoded revision=%d want=%d read error=%v encoding error=%v", value.GetFound(), encoded, sentinel.GetRevision(), err, encodeErr)
		return err == nil && encodeErr == nil && value.GetFound() && string(value.GetValue()) == key && encoded == sentinel.GetRevision()
	})

	// Kill every service while the writers are active. Do not replace any volume.
	recoveryDocker(t, ctx, c, "kill", "-s", "SIGKILL")
	stopWork()
	workers.Wait()
	load.counts(t, afterQuorumRestore)
	recoveryDocker(t, ctx, c, "start")
	router = recoveryClient(t, ctx, c, "router")
	waitRecoveryRouter(t, ctx, router)
	verifyAcknowledgedWrites(t, ctx, router, recoveryNamespace, load.journal)
}

func verifyAcknowledgedWrites(t *testing.T, ctx context.Context, router kvstorev1.KVStoreClient, namespace string, journal map[string]uint64) {
	t.Helper()
	total := len(journal)
	stream, err := router.Scan(ctx, &kvstorev1.ScanRequest{Namespace: namespace, UseVectorCursor: true})
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint bool
	for {
		record, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if record.GetCheckpoint() {
			checkpoint = record.GetCursor() != ""
			continue
		}
		if rev, exists := journal[record.GetKey()]; exists {
			if rev != record.GetRevision() || !bytes.Equal(record.GetValue(), []byte(record.GetKey())) {
				t.Fatalf("corrupt acknowledged record %s: revision=%d want %d", record.GetKey(), record.GetRevision(), rev)
			}
			delete(journal, record.GetKey())
		}
	}
	if !checkpoint || len(journal) != 0 {
		t.Fatalf("%d/%d acknowledged writes missing, scan checkpoint=%t", len(journal), total, checkpoint)
	}
	t.Logf("crash recovery verified: %d acknowledged keys, values and revisions", total)
}
