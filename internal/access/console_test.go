package access

import (
	"context"
	"net"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/apikeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestConsole(t *testing.T) {
	dir := t.TempDir()
	connection := startTestConsole(t, plaintextConsoleSettings(t, dir))
	client := kv.NewConsoleClient(connection)
	data := kv.NewKVStoreClient(connection)
	peer := kv.NewConsoleClient(startTestConsole(t, plaintextConsoleSettings(t, dir)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	withKey := func(secret string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+secret)
	}
	request := &kv.CreateAccessKeyRequest{Key: &kv.AccessKey{Name: "reader", Read: true, Namespaces: []string{"players"}}}
	_, err := client.CreateAccessKey(ctx, request)
	requireCode(t, err, codes.Unauthenticated)
	_, err = client.GetSession(ctx, &kv.GetSessionRequest{})
	requireCode(t, err, codes.Unauthenticated)
	_, err = client.ListNamespaces(ctx, &kv.ListNamespacesRequest{})
	requireCode(t, err, codes.Unauthenticated)
	_, err = client.ListAccessKeys(ctx, &kv.ListAccessKeysRequest{})
	requireCode(t, err, codes.Unauthenticated)
	_, err = client.RevokeAccessKey(ctx, &kv.RevokeAccessKeyRequest{Name: "reader"})
	requireCode(t, err, codes.Unauthenticated)
	_, err = client.GetSession(withKey("invalid"), &kv.GetSessionRequest{})
	requireCode(t, err, codes.Unauthenticated)

	manager := createTestKey(t, dir, apikeys.Key{Name: "manager", Manage: true})
	managerCtx := withKey(manager)
	session, err := client.GetSession(managerCtx, &kv.GetSessionRequest{})
	requireCode(t, err, codes.OK)
	if !proto.Equal(session.GetKey(), &kv.AccessKey{Name: "manager", Manage: true}) {
		t.Fatal("session did not return the manager's grants")
	}
	_, err = client.ListNamespaces(managerCtx, &kv.ListNamespacesRequest{})
	requireCode(t, err, codes.PermissionDenied)
	_, err = data.Get(managerCtx, &kv.GetRequest{Namespace: "players"})
	requireCode(t, err, codes.PermissionDenied)
	_, err = data.BatchWrite(managerCtx, &kv.BatchWriteRequest{Operations: []*kv.BatchWriteOperation{{Namespace: "players"}}})
	requireCode(t, err, codes.PermissionDenied)
	_, err = client.CreateAccessKey(managerCtx, &kv.CreateAccessKeyRequest{Key: &kv.AccessKey{Name: "no grants"}})
	requireCode(t, err, codes.InvalidArgument)

	issued, err := client.CreateAccessKey(managerCtx, request)
	requireCode(t, err, codes.OK)
	reader := issued.GetSecret()
	if _, ok := apikeys.Digest(reader); !ok {
		t.Fatal("create did not return a valid secret")
	}
	readerCtx := withKey(reader)
	_, err = client.CreateAccessKey(managerCtx, request)
	requireCode(t, err, codes.AlreadyExists)
	_, err = client.RevokeAccessKey(managerCtx, &kv.RevokeAccessKeyRequest{Name: "missing"})
	requireCode(t, err, codes.NotFound)
	_, err = data.Get(readerCtx, &kv.GetRequest{Namespace: "players"})
	requireCode(t, err, codes.OK)
	_, err = data.Get(readerCtx, &kv.GetRequest{Namespace: "other"})
	requireCode(t, err, codes.PermissionDenied)
	session, err = peer.GetSession(readerCtx, &kv.GetSessionRequest{})
	requireCode(t, err, codes.OK)
	if !proto.Equal(session.GetKey(), request.GetKey()) || strings.Contains(protojson.Format(session), reader) {
		t.Fatal("session did not return only the reader's grants")
	}
	names, err := client.ListNamespaces(readerCtx, &kv.ListNamespacesRequest{})
	requireCode(t, err, codes.OK)
	if !slices.Equal(names.GetNamespaces(), []string{"players"}) {
		t.Fatal("namespace discovery exposed namespaces outside the read grant")
	}
	_, err = client.ListAccessKeys(readerCtx, &kv.ListAccessKeysRequest{})
	requireCode(t, err, codes.PermissionDenied)
	_, err = client.CreateAccessKey(readerCtx, request)
	requireCode(t, err, codes.PermissionDenied)
	_, err = client.RevokeAccessKey(readerCtx, &kv.RevokeAccessKeyRequest{Name: "manager"})
	requireCode(t, err, codes.PermissionDenied)

	// Delegation grants management across the registry, independently of data scopes.
	delegated, err := client.CreateAccessKey(managerCtx, &kv.CreateAccessKeyRequest{Key: &kv.AccessKey{
		Name: "writer", Write: true, Manage: true, Namespaces: []string{"events"},
	}})
	requireCode(t, err, codes.OK)
	writerCtx := withKey(delegated.GetSecret())
	names, err = client.ListNamespaces(writerCtx, &kv.ListNamespacesRequest{})
	requireCode(t, err, codes.OK)
	if !slices.Equal(names.GetNamespaces(), []string{"events"}) {
		t.Fatal("namespace discovery did not respect the write grant")
	}
	keys, err := client.ListAccessKeys(writerCtx, &kv.ListAccessKeysRequest{})
	requireCode(t, err, codes.OK)
	if len(keys.GetKeys()) != 3 || keys.GetKeys()[0].GetName() != "manager" || keys.GetKeys()[1].GetName() != "reader" ||
		keys.GetKeys()[2].GetName() != "writer" || !keys.GetKeys()[2].GetManage() {
		t.Fatal("delegated manager did not receive the registry-wide grant list")
	}
	encoded := protojson.Format(keys)
	for _, forbidden := range []string{manager, reader, delegated.GetSecret(), "sha256"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatal("list response disclosed credential material")
		}
	}
	_, err = client.RevokeAccessKey(writerCtx, &kv.RevokeAccessKeyRequest{Name: "reader"})
	requireCode(t, err, codes.OK)
	_, err = data.Get(readerCtx, &kv.GetRequest{Namespace: "players"})
	requireCode(t, err, codes.Unauthenticated)
	_, err = peer.GetSession(readerCtx, &kv.GetSessionRequest{})
	requireCode(t, err, codes.Unauthenticated)

	if err := apikeys.Revoke(dir, "manager"); err != nil {
		t.Fatal(err)
	}
	_, err = client.CreateAccessKey(managerCtx, request)
	requireCode(t, err, codes.Unauthenticated)
	_, err = peer.RevokeAccessKey(managerCtx, &kv.RevokeAccessKeyRequest{Name: "writer"})
	requireCode(t, err, codes.Unauthenticated)
	_, err = peer.RevokeAccessKey(writerCtx, &kv.RevokeAccessKeyRequest{Name: "writer"})
	requireCode(t, err, codes.OK)
	_, err = client.CreateAccessKey(writerCtx, request)
	requireCode(t, err, codes.Unauthenticated)
	recovery := createTestKey(t, dir, apikeys.Key{Name: "recovery", Read: true, Write: true, Manage: true})
	_, err = client.ListAccessKeys(withKey(recovery), &kv.ListAccessKeysRequest{})
	requireCode(t, err, codes.OK)
}

func TestConsoleTrustedNamespaceDiscovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := kv.NewConsoleClient(startTestConsole(t, Settings{}))
	names, err := client.ListNamespaces(ctx, &kv.ListNamespacesRequest{})
	requireCode(t, err, codes.OK)
	if !slices.Equal(names.GetNamespaces(), []string{"events", "other", "players"}) {
		t.Fatal("trusted plaintext discovery did not return all namespaces")
	}
	_, err = client.GetSession(ctx, &kv.GetSessionRequest{})
	requireCode(t, err, codes.Unauthenticated)
	_, err = client.CreateAccessKey(ctx, &kv.CreateAccessKeyRequest{Key: &kv.AccessKey{Name: "untrusted", Manage: true}})
	requireCode(t, err, codes.Unauthenticated)
}

func TestConsoleCertificateNamespaceDiscovery(t *testing.T) {
	t.Setenv("KVSTORE_API_KEYS_DIR", t.TempDir())
	target, config, _ := startTestEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(credentials.NewTLS(config("spiffe://clients/reader", false))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := kv.NewConsoleClient(connection)
	names, err := client.ListNamespaces(ctx, &kv.ListNamespacesRequest{})
	requireCode(t, err, codes.OK)
	if !slices.Equal(names.GetNamespaces(), []string{"players"}) {
		t.Fatal("namespace discovery ignored the client certificate policy")
	}
	_, err = client.GetSession(ctx, &kv.GetSessionRequest{})
	requireCode(t, err, codes.Unauthenticated)
	_, err = client.ListAccessKeys(ctx, &kv.ListAccessKeysRequest{})
	requireCode(t, err, codes.Unauthenticated)
}

func TestConsoleReadOnlyRegistry(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires an unprivileged user and filesystem directory modes")
	}
	dir := t.TempDir()
	manager := createTestKey(t, dir, apikeys.Key{Name: "manager", Manage: true})
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
	client := kv.NewConsoleClient(startTestConsole(t, plaintextConsoleSettings(t, dir)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+manager)
	_, err := client.ListAccessKeys(ctx, &kv.ListAccessKeysRequest{})
	requireCode(t, err, codes.OK)
	_, err = client.CreateAccessKey(ctx, &kv.CreateAccessKeyRequest{Key: &kv.AccessKey{Name: "new", Manage: true}})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = client.RevokeAccessKey(ctx, &kv.RevokeAccessKeyRequest{Name: "manager"})
	requireCode(t, err, codes.FailedPrecondition)
}

func consoleTestNamespaces(context.Context) ([]string, error) {
	return []string{"players", "other", "events", "players"}, nil
}

func plaintextConsoleSettings(t *testing.T, dir string) Settings {
	t.Helper()
	t.Setenv("KVSTORE_API_KEYS_DIR", dir)
	for _, name := range []string{"KVSTORE_TLS_CERT_FILE", "KVSTORE_TLS_KEY_FILE", "KVSTORE_TLS_CLIENT_CA_FILE", "KVSTORE_ACCESS_POLICY_FILE"} {
		t.Setenv(name, "")
	}
	settings, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	return settings
}

func startTestConsole(t *testing.T, settings Settings) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(settings.ServerOptions()...)
	kv.RegisterKVStoreServer(server, &testService{})
	settings.RegisterConsole(server, consoleTestNamespaces)
	// With no periodic watch, these calls exercise synchronous refresh.
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}
