package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestClusterGetDoesNotWaitBehindFSMWriterLock(t *testing.T) {
	fsm, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fsm.Close()
	cmd, err := json.Marshal(command{Operation: opSet, Namespace: "test", Key: "key", Value: []byte("durable")})
	if err != nil {
		t.Fatal(err)
	}
	response, ok := fsm.Apply(&raft.Log{Type: raft.LogCommand, Index: 1, Term: 1, Data: cmd}).(applyResult)
	if !ok || response.Err != nil || response.Revision != 1 {
		t.Fatalf("apply failed: %+v", response)
	}

	// Simulate a writer holding f.mu across a Pebble WAL sync. GET must use
	// the previously published snapshot instead of waiting for f.mu.
	fsm.mu.Lock()
	done := make(chan error, 1)
	go func() {
		rec, found, err := fsm.Get(context.Background(), "test", "key")
		if err == nil && (!found || string(rec.Value) != "durable") {
			err = errUnexpectedSnapshotRead{}
		}
		done <- err
	}()
	select {
	case err := <-done:
		fsm.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		fsm.mu.Unlock()
		<-done
		t.Fatal("read blocked by FSM writer mutex")
	}
}

type errUnexpectedSnapshotRead struct{}

func (errUnexpectedSnapshotRead) Error() string {
	return "published snapshot did not contain the committed value"
}
