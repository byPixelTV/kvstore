//go:build integration

package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/clienttls"
	"github.com/byPixelTV/kvstore/internal/sharding"
	"google.golang.org/grpc"
)

// TestExternalHostRecovery runs the client side of a staged host-loss drill.
// The operator causes and records the actual host loss and replacement.
func TestExternalHostRecovery(t *testing.T) {
	addr := os.Getenv("KVSTORE_HOST_TEST_ADDR")
	if addr == "" {
		t.Skip("set KVSTORE_HOST_TEST_ADDR to the external staging router")
	}
	namespace := os.Getenv("KVSTORE_HOST_TEST_NAMESPACE")
	ca := os.Getenv("KVSTORE_HOST_TEST_TLS_CA")
	cert := os.Getenv("KVSTORE_HOST_TEST_TLS_CERT")
	key := os.Getenv("KVSTORE_HOST_TEST_TLS_KEY")
	if namespace == "" || ca == "" || cert == "" || key == "" {
		t.Fatal("an isolated staging namespace, server CA and client certificate/key are required")
	}
	window := 2 * time.Minute
	if text := os.Getenv("KVSTORE_HOST_TEST_WINDOW"); text != "" {
		var err error
		window, err = time.ParseDuration(text)
		if err != nil || window <= 0 {
			t.Fatal("KVSTORE_HOST_TEST_WINDOW must be a positive duration")
		}
	}
	creds, err := (clienttls.Options{CA: ca, Cert: cert, Key: key}).Credentials()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), window+2*time.Minute)
	defer cancel()
	client := kvstorev1.NewKVStoreClient(conn)
	waitRecoveryRouter(t, ctx, client)
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	load := &recoveryLoad{
		namespace: namespace, keyPrefix: fmt.Sprintf("host-test/%x/", id), journal: make(map[string]uint64),
	}
	workCtx, stopWork := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { stopWork(); workers.Wait() }()
	for shard := range sharding.ShardCount {
		workers.Go(func() { load.run(workCtx, client, shard) })
	}
	checkAllShards := func(phase int) bool {
		_, acks := load.counts(t, phase)
		for _, count := range acks {
			if count < 256 {
				return false
			}
		}
		return true
	}
	waitRecovery(t, ctx, "durable writes on every shard before host loss", func() bool {
		return checkAllShards(beforeLeaderCrash)
	})
	load.setPhase(afterLeaderCrash)
	t.Logf("HOST TEST WINDOW STARTED at %s for %s. Power off and replace the chosen staging host while writes continue.", time.Now().UTC().Format(time.RFC3339), window)
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	load.setPhase(afterQuorumRestore)
	waitRecovery(t, ctx, "durable writes on every shard after the host-loss window", func() bool {
		return checkAllShards(afterQuorumRestore)
	})
	stopWork()
	workers.Wait()
	attempts, acks := load.counts(t, afterLeaderCrash)
	t.Logf("fault window attempts=%v acknowledged=%v largest gaps between acknowledgements=%v", attempts, acks, load.maxAckGap)
	verifyAcknowledgedWrites(t, ctx, client, namespace, load.journal)
}
