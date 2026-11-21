package console

import (
	"bytes"
	"context"
	"net"
	"slices"
	"testing"
	"time"

	api "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/access"
	"github.com/byPixelTV/kvstore/internal/apikeys"
	"github.com/byPixelTV/kvstore/internal/grpcserver"
	"github.com/byPixelTV/kvstore/internal/kvstore"
	pebblestore "github.com/byPixelTV/kvstore/internal/storage/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConsoleClientWithLiveStore(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KVSTORE_API_KEYS_DIR", dir)
	for _, name := range []string{"KVSTORE_TLS_CERT_FILE", "KVSTORE_TLS_KEY_FILE", "KVSTORE_TLS_CLIENT_CA_FILE", "KVSTORE_ACCESS_POLICY_FILE"} {
		t.Setenv(name, "")
	}
	settings, err := access.Load()
	if err != nil {
		t.Fatal(err)
	}
	store, err := pebblestore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(settings.ServerOptions()...)
	api.RegisterKVStoreServer(server, grpcserver.New(kvstore.New(store)))
	settings.RegisterConsole(server, store.ListNamespaces)
	settings.WatchAPIKeys(t.Context())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	endpoint := Endpoint{Address: listener.Addr().String()}
	if connection, _, err := Open(ctx, endpoint, ""); err == nil {
		_ = connection.Close()
		t.Fatal("a console opened without an access key")
	}
	secret, err := apikeys.Create(dir, apikeys.Key{Name: "local operator", Read: true, Write: true, Manage: true})
	if err != nil {
		t.Fatal(err)
	}
	conn, session, err := Open(ctx, endpoint, secret)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if session.Name != "local operator" || !session.Manage || !session.Read || !session.Write {
		t.Fatalf("unexpected session permissions: %+v", session)
	}
	// A management RPC publishes the latest registry for the data endpoint.
	readerSecret, err := conn.CreateKey(ctx, AccessKey{Name: "reader", Read: true, Namespaces: []string{"players"}})
	if err != nil || readerSecret == "" {
		t.Fatalf("create reader: %v", err)
	}
	reader, _, err := Open(ctx, endpoint, readerSecret)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if keys, err := conn.ListKeys(ctx); err != nil || len(keys) != 2 {
		t.Fatalf("list keys: got %d, %v", len(keys), err)
	}
	if _, err := reader.ListKeys(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reader obtained key administration: %v", err)
	}
	deadline := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	written, err := conn.Set(ctx, Record{Namespace: "players", Key: "inventory", Value: []byte{0, 1, 255}, ExpiresAt: deadline})
	if err != nil || !written.ExpiresAt.Equal(deadline) || written.Revision == 0 {
		t.Fatalf("write expiring value: %+v, %v", written, err)
	}
	result, err := conn.Batch(ctx, []Mutation{{Namespace: "players", Key: "name", Value: []byte("Avery")}, {Namespace: "audit", Key: "1", Value: []byte("log")}})
	if err != nil || len(result) != 2 || result[0].Revision == 0 || result[1].Revision == 0 || result[0].Error != "" || result[1].Error != "" {
		t.Fatalf("batch result: %+v, %v", result, err)
	}
	if names, err := conn.Namespaces(ctx); err != nil || !slices.Equal(names, []string{"audit", "players"}) {
		t.Fatalf("list live namespaces: %v, %v", names, err)
	}
	if names, err := reader.Namespaces(ctx); err != nil || !slices.Equal(names, []string{"players"}) {
		t.Fatalf("list scoped namespaces: %v, %v", names, err)
	}
	scan, err := reader.Scan(ctx, "players", 1)
	if err != nil || !scan.More || len(scan.Records) != 1 || scan.Records[0].Key != "inventory" || scan.Records[0].Value != nil {
		t.Fatalf("browse keys: %+v, %v", scan, err)
	}
	value, found, err := reader.Get(ctx, "players", "inventory")
	if err != nil || !found || value.Revision != written.Revision || !bytes.Equal(value.Value, []byte{0, 1, 255}) {
		t.Fatalf("read binary value: %+v, %t, %v", value, found, err)
	}
	if _, _, err := reader.Get(ctx, "audit", "1"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reader accessed another namespace: %v", err)
	}
	watch, err := reader.Watch(ctx, "players", "inventory")
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := conn.Delete(ctx, "players", "inventory"); err != nil || !deleted {
		t.Fatalf("delete inventory: %t, %v", deleted, err)
	}
	for {
		event, err := watch.Next()
		if err != nil {
			t.Fatal(err)
		}
		if event.Checkpoint {
			continue
		}
		if !event.Deleted || event.Record.Key != "inventory" || event.Record.Revision <= written.Revision {
			t.Fatalf("unexpected watch event: %+v", event)
		}
		break
	}
	if err := conn.RevokeKey(ctx, "reader"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.Get(ctx, "players", "name"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("revoked key still accepted: %v", err)
	}
}
