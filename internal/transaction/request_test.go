package transaction

import (
	"fmt"
	"testing"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParseRequestKeepsLargeConditionalCommitTogether(t *testing.T) {
	req := &pb.TransactionRequest{RequestId: "large_commit_0001"}
	for i := range 300 {
		key := fmt.Sprintf("claim-%d", i)
		req.Checks = append(req.Checks, &pb.TransactionCheck{
			Namespace: "claims", Key: key, Expected: &pb.TransactionCheck_Revision{Revision: uint64(i)},
		}, &pb.TransactionCheck{
			Namespace: "kvstore-leases", Key: key, Expected: &pb.TransactionCheck_Value{Value: []byte("owner")},
		})
		req.Operations = append(req.Operations, &pb.BatchWriteOperation{Namespace: "claims", Key: key, Value: []byte("updated")})
	}
	plan, err := ParseRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Parts) != 1 || len(plan.Parts[0].Checks) != 600 || len(plan.Parts[0].Writes) != 300 {
		t.Fatalf("unexpected transaction partition: %+v", plan)
	}
	retry, err := ParseRequest(req)
	if err != nil || retry.Fingerprint != plan.Fingerprint {
		t.Fatalf("identical request fingerprint changed: %v", err)
	}
	req.Operations[0].Namespace = "__kvstore_internal_routing"
	if _, err := ParseRequest(req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("internal namespace accepted: %v", err)
	}
}
