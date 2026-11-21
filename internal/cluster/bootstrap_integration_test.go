//go:build integration

package cluster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestExplicitRaftReplacesBootstrapVoterFromEmptyStorage(t *testing.T) {
	configs := explicitClusterConfigs(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	var nodes [3]*Node
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.Close()
			}
		}
	})
	start := func(cfgIndex int) {
		t.Helper()
		n := openBootstrapTestNode(t, configs[cfgIndex])
		nodes[cfgIndex] = n
		go n.RegisterSelf(ctx)
		go n.JoinUntilMember(ctx)
	}
	stop := func(cfgIndex int) {
		t.Helper()
		if err := nodes[cfgIndex].Close(); err != nil {
			t.Fatal(err)
		}
		nodes[cfgIndex] = nil
	}
	waitFor := func(what string, check func() bool) {
		t.Helper()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for !check() {
			select {
			case <-ctx.Done():
				t.Fatalf("timed out waiting for %s: %v", what, ctx.Err())
			case <-ticker.C:
			}
		}
	}

	for i := range nodes {
		start(i)
	}
	auth := "Bearer " + bootstrapTestToken
	bootstrapAddr := configs[0].AdminAdvertiseAddr
	requireBootstrapStatus(t, http.MethodPost, bootstrapAddr, auth, http.StatusNoContent)
	waitFor("all three voters ready", func() bool {
		for _, n := range nodes {
			n.RefreshAdmission()
			if !n.Ready() || !n.member() {
				return false
			}
		}
		return nodes[0].raft.State() == raft.Leader
	})
	expiresAt := time.Now().Add(time.Hour).UnixMilli()
	first, err := nodes[0].SetWithExpiration(ctx, "recovery", "before", []byte("retained"), expiresAt)
	if err != nil || first == 0 {
		t.Fatalf("initial write revision=%d error=%v", first, err)
	}

	stop(0)
	var leader *Node
	waitFor("a surviving quorum leader", func() bool {
		for _, n := range nodes[1:] {
			n.RefreshAdmission()
			if n.ClientTrafficReady() && n.raft.State() == raft.Leader {
				leader = n
				return true
			}
		}
		return false
	})
	second, err := leader.Set(ctx, "recovery", "during", []byte("quorum"))
	if err != nil || second != first+1 {
		t.Fatalf("write with designated voter down revision=%d error=%v", second, err)
	}
	// Compact the leader's log so a blank replacement must use the snapshot.
	reload := leader.raft.ReloadableConfig()
	reload.TrailingLogs = 0
	if err := leader.raft.ReloadConfig(reload); err != nil {
		t.Fatal(err)
	}
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	if index, err := leader.raftStore.FirstIndex(); err != nil || index == 1 {
		t.Fatalf("snapshot did not compact the original configuration index: %d, %v", index, err)
	}

	configs[0].DataDir = t.TempDir()
	isolatedCfg := configs[0]
	// Make the first request while the replacement is demonstrably blank.
	// Survivors retain their stable Raft address for node0 and cannot send logs
	// to this temporarily different socket.
	isolatedCfg.RaftBindAddr = freeTCPAddr(t)
	nodes[0] = openBootstrapTestNode(t, isolatedCfg)
	requireBootstrapStatus(t, http.MethodGet, bootstrapAddr, auth, http.StatusOK)
	requireBootstrapStatus(t, http.MethodPost, bootstrapAddr, auth, http.StatusConflict)
	if nodes[0].raft.LastIndex() != 0 || nodes[0].voterCount() != 0 {
		t.Fatal("operator request initialized the empty replacement alongside the existing group")
	}
	stop(0)

	start(0)
	replacement := nodes[0]
	waitFor("snapshot and applied data on the empty replacement", func() bool {
		replacement.RefreshAdmission()
		rec, found, err := replacement.GetLocal(ctx, "recovery", "before")
		during, duringFound, duringErr := replacement.GetLocal(ctx, "recovery", "during")
		return replacement.Ready() && replacement.member() && replacement.voterCount() == 3 &&
			err == nil && found && string(rec.Value) == "retained" &&
			rec.Revision == first && rec.ExpiresAtUnixMS == expiresAt &&
			duringErr == nil && duringFound && string(during.Value) == "quorum" && during.Revision == second
	})
	list, err := replacement.snapshots.List()
	if err != nil || len(list) == 0 {
		t.Fatalf("replacement did not install a snapshot: %v, %v", list, err)
	}

	// Take the current leader down so restoring quorum requires the replacement.
	if leader == nodes[1] {
		stop(1)
	} else {
		stop(2)
	}
	waitFor("leadership with the replacement voter", func() bool {
		for _, n := range nodes {
			if n != nil {
				n.RefreshAdmission()
				if n.ClientTrafficReady() && n.raft.State() == raft.Leader {
					leader = n
					return true
				}
			}
		}
		return false
	})
	third, err := leader.Set(ctx, "recovery", "after", []byte("restored quorum"))
	if err != nil || third != second+1 {
		t.Fatalf("write with replacement voter revision=%d error=%v", third, err)
	}
}
