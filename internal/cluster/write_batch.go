package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	clusterWriteBatchWindow   = 250 * time.Microsecond
	clusterWriteBatchMax      = 64
	clusterWriteQueueCapacity = 4096
	// Conservative estimated JSON payload limit (Raft's recommended log
	// entry size is around 512 KiB). A 256 KiB value alone fits.
	clusterWriteBatchBytes = 400 << 10
)

var errClusterBatchClosed = errors.New("cluster write batcher is closed")

type clusterWriteRequest struct {
	ctx   context.Context
	cmd   command
	reply chan writeResponse
}

type clusterWriteBatcher struct {
	node   *Node
	gate   sync.RWMutex
	closed bool
	queue  chan clusterWriteRequest
	done   chan struct{}
}

func newClusterWriteBatcher(node *Node) *clusterWriteBatcher {
	b := &clusterWriteBatcher{node: node, queue: make(chan clusterWriteRequest, clusterWriteQueueCapacity), done: make(chan struct{})}
	go b.loop()
	return b
}

func (b *clusterWriteBatcher) Submit(ctx context.Context, cmd command) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	cmd.Value = bytes.Clone(cmd.Value)
	req := clusterWriteRequest{ctx: ctx, cmd: cmd, reply: make(chan writeResponse, 1)}
	b.gate.RLock()
	if b.closed {
		b.gate.RUnlock()
		return 0, errClusterBatchClosed
	}
	select {
	case b.queue <- req:
		b.gate.RUnlock()
	case <-ctx.Done():
		b.gate.RUnlock()
		return 0, ctx.Err()
	}
	select {
	case result := <-req.reply:
		return result.revision, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (b *clusterWriteBatcher) Close() {
	b.gate.Lock()
	if !b.closed {
		b.closed = true
		close(b.queue)
	}
	b.gate.Unlock()
	<-b.done
}

func estimateRaftCommand(c command) int {
	return 128 + len(c.Namespace) + len(c.Key) + 4*(len(c.Value)+2)/3
}

func (b *clusterWriteBatcher) loop() {
	defer close(b.done)
	var pending *clusterWriteRequest
	for {
		var first clusterWriteRequest
		if pending != nil {
			first = *pending
			pending = nil
		} else {
			next, ok := <-b.queue
			if !ok {
				return
			}
			first = next
		}
		group := []clusterWriteRequest{first}
		totalBytes := estimateRaftCommand(first.cmd)
		timer := time.NewTimer(clusterWriteBatchWindow)
		queueClosed := false
	collect:
		for len(group) < clusterWriteBatchMax {
			select {
			case next, ok := <-b.queue:
				if !ok {
					queueClosed = true
					break collect
				}
				estimated := estimateRaftCommand(next.cmd)
				if totalBytes+estimated > clusterWriteBatchBytes {
					pending = &next
					break collect
				}
				group = append(group, next)
				totalBytes += estimated
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		b.applyGroup(group)
		if queueClosed && pending == nil {
			return
		}
	}
}

func (b *clusterWriteBatcher) applyGroup(group []clusterWriteRequest) {
	results := make([]writeResponse, len(group))
	commands := make([]command, 0, len(group))
	positions := make([]int, 0, len(group))
	for i, req := range group {
		if err := req.ctx.Err(); err != nil {
			results[i].err = err
			continue
		}
		commands = append(commands, req.cmd)
		positions = append(positions, i)
	}
	if len(commands) > 0 {
		var cmd command
		if len(commands) == 1 {
			cmd = commands[0]
		} else {
			cmd = command{Operation: opBatch, Commands: commands}
		}
		// A client cancel after submitting a Raft entry cannot undo a commit.
		// The batch belongs to the server and is not tied to one caller's ctx.
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		response, err := b.node.applyDirect(ctx, cmd)
		cancel()
		if err == nil && len(commands) > 1 && len(response.Revisions) != len(commands) {
			err = fmt.Errorf("raft batch returned %d revisions for %d commands", len(response.Revisions), len(commands))
		}
		for j, idx := range positions {
			if err != nil {
				results[idx].err = err
				continue
			}
			if len(commands) == 1 {
				results[idx].revision = response.Revision
			} else {
				results[idx].revision = response.Revisions[j]
			}
		}
	}
	for i, req := range group {
		req.reply <- results[i]
	}
}
