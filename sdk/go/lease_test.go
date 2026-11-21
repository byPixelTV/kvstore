package kvstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	sdk "github.com/byPixelTV/kvstore/sdk/go"
	"google.golang.org/grpc"
)

func TestShortLeaseRenewsWithShardedPreparationMargin(t *testing.T) {
	transport := &delayedLeaseTransport{renewed: make(chan struct{}, 1)}
	client := sdk.FromConn(transport, "")
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	err := client.WithKeyLock(ctx, "sessions", "id", 5*time.Second, func(ctx context.Context) error {
		select {
		case <-transport.renewed:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
	if err != nil {
		t.Fatalf("short lease lost during delayed renewal or release: %v", err)
	}
}

type delayedLeaseTransport struct {
	value   []byte
	expires int64
	renewed chan struct{}
}

func (s *delayedLeaseTransport) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	switch method {
	case pb.KVStore_Get_FullMethodName:
		if len(s.value) != 0 && time.Now().UnixMilli() < s.expires {
			*reply.(*pb.GetResponse) = pb.GetResponse{Found: true, Value: s.value, ExpiresAtUnixMs: s.expires}
		}
		return nil
	case pb.KVStore_Transact_FullMethodName:
		request := args.(*pb.TransactionRequest)
		check, write := request.GetChecks()[0], request.GetOperations()[0]
		response := reply.(*pb.TransactionResponse)
		if owner := check.GetValue(); len(owner) != 0 {
			// Model the three-second sharded preparation window and a
			// renewal admitted 500ms after its client sent it.
			prepareAt := time.Now()
			if !write.GetDelete() {
				prepareAt = prepareAt.Add(500 * time.Millisecond)
			}
			if !bytes.Equal(s.value, owner) || s.expires <= prepareAt.Add(3*time.Second).UnixMilli() {
				return nil
			}
		}
		s.value, s.expires = write.GetValue(), write.GetExpiresAtUnixMs()
		response.Committed = true
		if len(check.GetValue()) != 0 && !write.GetDelete() {
			select {
			case s.renewed <- struct{}{}:
			default:
			}
		}
		return nil
	default:
		return fmt.Errorf("unexpected RPC %s", method)
	}
}

func (*delayedLeaseTransport) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected streaming RPC")
}
