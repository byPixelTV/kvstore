package cluster

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/byPixelTV/kvstore/internal/sharding"
	"github.com/hashicorp/raft"
)

func TestMicrobatchFencesOnlyAffectedPointWrite(t *testing.T) {
	f, err := openFSM(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sealedSlot := sharding.Slot("n", "sealed")
	safeKey := "safe"
	for sharding.Slot("n", safeKey) == sealedSlot {
		safeKey += "-next"
	}
	fence, _ := json.Marshal(command{Operation: opFence, Namespace: "n", Slot: uint32(sealedSlot), Sealed: true})
	res := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 1, Data: fence}).(applyResult)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	// The real router may still have outstanding point requests when the
	// fence lands. Only the fenced request must fail.
	cmd, _ := json.Marshal(command{Operation: opMicrobatch, Commands: []command{
		{Operation: opSet, Namespace: "n", Key: "sealed", Value: []byte("no")},
		{Operation: opSet, Namespace: "n", Key: safeKey, Value: []byte("yes")},
	}})
	res = f.Apply(&raft.Log{Type: raft.LogCommand, Index: 2, Data: cmd}).(applyResult)
	if res.Err != nil || len(res.Revisions) != 2 || len(res.Errors) != 2 {
		t.Fatalf("bad result %+v", res)
	}
	if res.Errors[0] == nil || res.Errors[1] != nil || res.Revisions[0] != 0 || res.Revisions[1] != 1 {
		t.Fatalf("invalid split results: %+v", res)
	}
	if _, exists, err := f.Get(context.Background(), "n", "sealed"); err != nil || exists {
		t.Fatalf("sealed key visible: %v %v", exists, err)
	}
	if got, exists, err := f.Get(context.Background(), "n", safeKey); err != nil || !exists || string(got.Value) != "yes" {
		t.Fatalf("good key lost: %+v %v %v", got, exists, err)
	}
}
