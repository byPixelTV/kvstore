package cluster

import (
	"errors"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
)

// leaderFence amortizes the expensive Raft log barrier across all reads in
// one leadership term. VerifyLeader is still required for EVERY read batch.
//
// A barrier on the first read after an election is essential: VerifyLeader
// alone does not guarantee that the newly elected leader has applied the
// committed prefix from earlier terms. All successful KVStore writes already
// wait for FSM Apply before replying, so after this initial barrier there is
// no need to append a fresh log barrier for each subsequent read.
//
// This optimization assumes all STRONG writes go through this Raft FSM and
// their replies are sent only after the corresponding future is applied.
type leaderFence struct {
	mu              sync.Mutex
	initializedTerm uint64
	term            func() (uint64, error)
	barrier         func() error
	verify          func() error
}

func (g *leaderFence) Fence() error {
	term, err := g.term()
	if err != nil {
		return err
	}
	if term == 0 {
		return errors.New("cannot verify Raft term zero")
	}

	g.mu.Lock()
	if g.initializedTerm != term {
		if err = g.barrier(); err == nil {
			var after uint64
			after, err = g.term()
			if err == nil && after != term {
				err = fmt.Errorf("Raft term changed during initial read barrier: %d -> %d", term, after)
			}
		}
		if err == nil {
			g.initializedTerm = term
		}
	}
	g.mu.Unlock()
	if err != nil {
		return err
	}

	if err = g.verify(); err != nil {
		return err
	}
	after, err := g.term()
	if err != nil {
		return err
	}
	if after != term {
		return fmt.Errorf("Raft term changed during read verification: %d -> %d", term, after)
	}
	return nil
}

// raftTerm uses Raft.CurrentTerm(), a cheap atomic read, rather than
// constructing the full diagnostic Stats map on every read batch.
func (n *Node) raftTerm() (uint64, error) {
	if n.raft.State() != raft.Leader {
		return 0, raft.ErrNotLeader
	}
	term := n.raft.CurrentTerm()
	if n.raft.State() != raft.Leader {
		return 0, raft.ErrNotLeader
	}
	return term, nil
}
