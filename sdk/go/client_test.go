package kvstore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	pebble "github.com/byPixelTV/kvstore/internal/storage/pebble"
	sdk "github.com/byPixelTV/kvstore/sdk/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func testClient(t *testing.T) *sdk.Client {
	t.Helper()
	if target := os.Getenv("KVSTORE_SDK_TEST_TARGET"); target != "" {
		client, err := sdk.Dial(target, sdk.Options{Plaintext: true, APIKey: os.Getenv("KVSTORE_SDK_TEST_KEY")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	store, err := pebble.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterKVStoreServer(server, grpcserver.New(kvstore.New(store)))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///kvstore", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return sdk.FromConn(conn, "")
}

func TestSDKChangefeed(t *testing.T) {
	client := testClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	namespace := fmt.Sprintf("sdk-go-%d", time.Now().UnixNano())
	type Player struct{ Coins int }
	head, err := client.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sdk.SetJSON(ctx, client, namespace, "alice", Player{Coins: 7}, 0); err != nil {
		t.Fatal(err)
	}
	value, found, err := sdk.GetJSON[Player](ctx, client, namespace, "alice")
	if err != nil || !found || value.Coins != 7 {
		t.Fatalf("get JSON = %+v, %t, %v", value, found, err)
	}
	scan, err := client.Scan(ctx, namespace)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot *pb.ScanResponse
	var sawValue bool
	for {
		item, err := scan.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if item.Checkpoint {
			snapshot = item
		} else if item.Key == "alice" {
			sawValue = true
		}
	}
	if snapshot == nil || !sawValue {
		t.Fatalf("incomplete snapshot: checkpoint=%v value=%t", snapshot, sawValue)
	}

	results, err := client.BatchWrite(ctx, &pb.BatchWriteOperation{Namespace: namespace, Key: "alice", Delete: true})
	if err != nil || len(results) != 1 || !results[0].Deleted || results[0].GrpcCode != 0 {
		t.Fatalf("delete result=%v err=%v", results, err)
	}
	cursor := sdk.CursorFromScan(snapshot)
	handlerErr := errors.New("stop without committing event")
	resume, err := client.OnChange(ctx, namespace, "alice", cursor, func(change *pb.WatchResponse) error {
		if !change.Deleted {
			t.Errorf("expected delete event, got %v", change)
		}
		return handlerErr
	})
	if !errors.Is(err, handlerErr) || resume != cursor {
		t.Fatalf("handler error must leave the cursor before the failed event: %v %v", resume, err)
	}

	start := sdk.CursorFromHead(head)
	stream, err := client.Watch(ctx, namespace, "alice", &start)
	if err != nil {
		t.Fatal(err)
	}
	for _, deleted := range []bool{false, true} {
		for {
			change, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if change.Checkpoint {
				continue
			}
			if change.Key != "alice" || change.Deleted != deleted {
				t.Fatalf("unexpected change: %v", change)
			}
			break
		}
	}
}
