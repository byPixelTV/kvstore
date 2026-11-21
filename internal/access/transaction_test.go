package access

import (
	"testing"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
)

func TestTransactionRequiresGrantsOnEveryNamespace(t *testing.T) {
	grants := namespaceGrants{read: map[string]bool{"leases": true}, write: map[string]bool{"orders": true}}
	req := &pb.TransactionRequest{
		Checks:     []*pb.TransactionCheck{{Namespace: "leases"}},
		Operations: []*pb.BatchWriteOperation{{Namespace: "orders"}},
	}
	if !grants.allows(req) {
		t.Fatal("expected permitted transaction")
	}
	req.Checks = append(req.Checks, &pb.TransactionCheck{Namespace: "private"})
	if grants.allows(req) {
		t.Fatal("transaction granted an unauthorized comparison")
	}
	req.Checks = req.Checks[:1]
	req.Operations = append(req.Operations, &pb.BatchWriteOperation{Namespace: "private"})
	if grants.allows(req) {
		t.Fatal("transaction granted an unauthorized write")
	}
	if grants.allows(&pb.TransactionControlRequest{}) {
		t.Fatal("data grants enabled internal transaction control")
	}
}
