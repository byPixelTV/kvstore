package transaction

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	lifetime          = 3 * time.Second
	maxItems          = 4096
	MaxControlPayload = 3 << 20
)

var requestID = regexp.MustCompile(`^[a-zA-Z0-9_-]{16,64}$`)

func ParseRequest(req *pb.TransactionRequest) (Plan, error) {
	if !requestID.MatchString(req.GetRequestId()) {
		return Plan{}, status.Error(codes.InvalidArgument, "invalid transaction request ID")
	}
	checks, ops := req.GetChecks(), req.GetOperations()
	if len(checks)+len(ops) == 0 || len(checks) > maxItems || len(ops) > maxItems {
		return Plan{}, status.Error(codes.InvalidArgument, "transaction requires at least one check or write and allows up to 4096 per list")
	}
	if proto.Size(req) > 2<<20 {
		return Plan{}, status.Error(codes.ResourceExhausted, "transaction exceeds 2 MiB")
	}
	part := Part{Checks: make([]Check, 0, len(checks)), Writes: make([]storage.WriteOperation, 0, len(ops))}
	for _, item := range checks {
		if !validKey(item.GetNamespace(), item.GetKey()) {
			return Plan{}, status.Error(codes.InvalidArgument, "invalid transaction check key")
		}
		check := Check{Namespace: item.GetNamespace(), Key: item.GetKey()}
		switch expected := item.GetExpected().(type) {
		case *pb.TransactionCheck_Revision:
			check.Revision = expected.Revision
		case *pb.TransactionCheck_Value:
			check.MatchValue, check.Value = true, expected.Value
		default:
			return Plan{}, status.Error(codes.InvalidArgument, "a transaction check requires an expected revision or value")
		}
		part.Checks = append(part.Checks, check)
	}
	for _, item := range ops {
		if !validKey(item.GetNamespace(), item.GetKey()) || item.GetExpiresAtUnixMs() < 0 ||
			(item.GetDelete() && (item.GetExpiresAtUnixMs() != 0 || len(item.GetValue()) != 0)) {
			return Plan{}, status.Error(codes.InvalidArgument, "invalid transaction write")
		}
		part.Writes = append(part.Writes, storage.WriteOperation{
			Namespace: item.GetNamespace(), Key: item.GetKey(), Value: item.GetValue(),
			Delete: item.GetDelete(), ExpiresAtUnixMS: item.GetExpiresAtUnixMs(),
		})
	}
	// Router controls encode bytes in JSON, including base64 values and
	// escaped keys. Leave room for routing groups and coordinator metadata.
	payload, err := json.Marshal(part)
	if err != nil || len(payload) > MaxControlPayload-2048 {
		return Plan{}, status.Error(codes.ResourceExhausted, "transaction exceeds the coordinator payload limit")
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(req)
	if err != nil {
		return Plan{}, status.Error(codes.InvalidArgument, "invalid transaction request encoding")
	}
	hash := sha256.Sum256(wire)
	return Plan{ID: req.GetRequestId(), Fingerprint: hex.EncodeToString(hash[:]), DeadlineMS: time.Now().Add(lifetime).UnixMilli(), Parts: []Part{part}}, nil
}

func validKey(namespace, key string) bool {
	return namespace != "" && key != "" && !strings.HasPrefix(namespace, "__kvstore_internal_")
}
