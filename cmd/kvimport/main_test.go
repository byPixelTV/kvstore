package main

import (
	"context"
	"testing"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
)

type importTarget struct {
	kv.KVStoreClient
	values map[string]string
	writes int
}

func (f *importTarget) Get(_ context.Context, req *kv.GetRequest, _ ...grpc.CallOption) (*kv.GetResponse, error) {
	value, found := f.values[req.GetKey()]
	return &kv.GetResponse{Found: found, Value: []byte(value)}, nil
}

func (f *importTarget) Set(_ context.Context, _ *kv.SetRequest, _ ...grpc.CallOption) (*kv.SetResponse, error) {
	f.writes++
	return &kv.SetResponse{Revision: 1}, nil
}

func TestDryRunCountsDestinationRecordsWithoutWriting(t *testing.T) {
	target := &importTarget{values: map[string]string{"matching": "value", "conflict": "different"}}
	var dry totals
	for _, key := range []string{"new", "matching", "conflict"} {
		record := &kv.SetRequest{Namespace: "store", Key: key, Value: []byte("value")}
		if err := compareRecord(t.Context(), target, record, false, &dry); err != nil {
			t.Fatal(err)
		}
	}
	if dry.newKeys != 1 || dry.unchanged != 1 || dry.conflicts != 1 || target.writes != 0 {
		t.Fatalf("unexpected dry run result %+v with %d writes", dry, target.writes)
	}

	var applied totals
	record := &kv.SetRequest{Namespace: "store", Key: "new", Value: []byte("value")}
	if err := compareRecord(t.Context(), target, record, true, &applied); err != nil {
		t.Fatal(err)
	}
	if applied.written != 1 || target.writes != 1 {
		t.Fatalf("apply did not perform the expected write: %+v", applied)
	}
}
