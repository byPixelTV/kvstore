package access

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testService struct {
	kv.UnimplementedKVStoreServer
	batches      atomic.Int32
	unaryActive  chan struct{}
	streamActive chan struct{}
}

func (s *testService) Get(ctx context.Context, req *kv.GetRequest) (*kv.GetResponse, error) {
	if req.GetKey() == "hold" {
		close(s.unaryActive)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &kv.GetResponse{Found: true}, nil
}

func (s *testService) Head(context.Context, *kv.HeadRequest) (*kv.HeadResponse, error) {
	return &kv.HeadResponse{}, nil
}

func (s *testService) BatchWrite(context.Context, *kv.BatchWriteRequest) (*kv.BatchWriteResponse, error) {
	s.batches.Add(1)
	return &kv.BatchWriteResponse{}, nil
}

func (s *testService) Scan(_ *kv.ScanRequest, stream grpc.ServerStreamingServer[kv.ScanResponse]) error {
	return stream.Send(&kv.ScanResponse{Key: "record"})
}

func (s *testService) Watch(_ *kv.WatchRequest, stream grpc.ServerStreamingServer[kv.WatchResponse]) error {
	close(s.streamActive)
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestMutualTLSNamespacePolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target, tlsConfig, service := startTestEndpoint(t)
	client := connect(t, target, tlsConfig("spiffe://clients/player", false))
	get := func(client kv.KVStoreClient, ctx context.Context, namespace string) error {
		_, err := client.Get(ctx, &kv.GetRequest{Namespace: namespace})
		return err
	}
	requireCode(t, get(client, ctx, "players"), codes.OK)
	requireCode(t, get(client, ctx, "other"), codes.PermissionDenied)
	requireCode(t, get(connect(t, target, tlsConfig("", false)), ctx, "players"), codes.Unavailable)
	requireCode(t, get(connect(t, target, tlsConfig("spiffe://clients/player", true)), ctx, "players"), codes.Unavailable)
	spoof := metadata.AppendToOutgoingContext(ctx, "x-forwarded-client-cert", "spiffe://clients/player")
	requireCode(t, get(connect(t, target, tlsConfig("spiffe://clients/unknown", false)), spoof, "players"), codes.PermissionDenied)
	_, err := client.Head(ctx, &kv.HeadRequest{})
	requireCode(t, err, codes.OK)
	reader := connect(t, target, tlsConfig("spiffe://clients/reader", false))
	_, err = reader.Head(ctx, &kv.HeadRequest{})
	requireCode(t, err, codes.PermissionDenied)
	request := &kv.BatchWriteRequest{Operations: []*kv.BatchWriteOperation{{Namespace: "players"}, {Namespace: "other"}}}
	_, err = client.BatchWrite(ctx, request)
	requireCode(t, err, codes.PermissionDenied)
	if count := service.batches.Load(); count != 0 {
		t.Fatalf("denied batch reached storage %d times", count)
	}
	request.Operations = request.Operations[:1]
	_, err = reader.BatchWrite(ctx, request)
	requireCode(t, err, codes.PermissionDenied)
	_, err = client.BatchWrite(ctx, request)
	requireCode(t, err, codes.OK)
	if count := service.batches.Load(); count != 1 {
		t.Fatalf("authorized batch reached storage %d times", count)
	}
	for namespace, want := range map[string]codes.Code{"players": codes.OK, "other": codes.PermissionDenied} {
		stream, err := client.Scan(ctx, &kv.ScanRequest{Namespace: namespace})
		requireCode(t, err, codes.OK)
		_, err = stream.Recv()
		requireCode(t, err, want)
	}
	watch, err := client.Watch(ctx, &kv.WatchRequest{Namespace: "other"})
	requireCode(t, err, codes.OK)
	_, err = watch.Recv()
	requireCode(t, err, codes.PermissionDenied)
}

func TestAdmissionLimitsProtectUnaryAndStreamingSeparately(t *testing.T) {
	t.Setenv("KVSTORE_MAX_INFLIGHT_RPCS", "1")
	t.Setenv("KVSTORE_MAX_STREAMS", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target, tlsConfig, service := startTestEndpoint(t)
	client := connect(t, target, tlsConfig("spiffe://clients/player", false))
	unaryDone := make(chan error, 1)
	go func() {
		_, err := client.Get(ctx, &kv.GetRequest{Namespace: "players", Key: "hold"})
		unaryDone <- err
	}()
	waitActive(t, ctx, service.unaryActive)
	_, err := client.BatchWrite(ctx, &kv.BatchWriteRequest{Operations: []*kv.BatchWriteOperation{{Namespace: "players"}}})
	requireCode(t, err, codes.ResourceExhausted)
	watch, err := client.Watch(ctx, &kv.WatchRequest{Namespace: "players"})
	requireCode(t, err, codes.OK)
	waitActive(t, ctx, service.streamActive)
	scan, err := client.Scan(ctx, &kv.ScanRequest{Namespace: "players"})
	requireCode(t, err, codes.OK)
	_, err = scan.Recv()
	requireCode(t, err, codes.ResourceExhausted)
	cancel()
	requireCode(t, <-unaryDone, codes.Canceled)
	_, err = watch.Recv()
	requireCode(t, err, codes.Canceled)
	if count := service.batches.Load(); count != 0 {
		t.Fatalf("over-limit batch reached storage %d times", count)
	}
}

func TestLoadRejectsInsecurePolicyAndMalformedLimit(t *testing.T) {
	t.Setenv("KVSTORE_ACCESS_POLICY_FILE", "policy.json")
	if _, err := Load(); err == nil {
		t.Fatal("policy without mTLS was accepted")
	}
	t.Setenv("KVSTORE_ACCESS_POLICY_FILE", "")
	t.Setenv("KVSTORE_MAX_INFLIGHT_RPCS", "-1")
	if _, err := Load(); err == nil {
		t.Fatal("negative limit was accepted")
	}
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if code := status.Code(err); code != want {
		t.Fatalf("gRPC returned %v (%v), want %v", code, err, want)
	}
}

func waitActive(t *testing.T, ctx context.Context, active <-chan struct{}) {
	t.Helper()
	select {
	case <-active:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func startTestEndpoint(t *testing.T) (string, func(string, bool) *tls.Config, *testService) {
	t.Helper()
	ca, caKey, caPEM := newCertificate(t, nil, nil, 1, "")
	wrong, wrongKey, _ := newCertificate(t, nil, nil, 2, "")
	_, serverKey, serverPEM := newCertificate(t, ca, caKey, 3, "")
	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeFile := func(filename string, content []byte) string {
		path := filepath.Join(dir, filename)
		if err := os.WriteFile(path, content, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	t.Setenv("KVSTORE_TLS_CERT_FILE", writeFile("server.pem", serverPEM))
	t.Setenv("KVSTORE_TLS_KEY_FILE", writeFile("server.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER})))
	t.Setenv("KVSTORE_TLS_CLIENT_CA_FILE", writeFile("ca.pem", caPEM))
	t.Setenv("KVSTORE_HEALTH_LISTEN_ADDR", "127.0.0.1:0")
	t.Setenv("KVSTORE_ACCESS_POLICY_FILE", writeFile("policy.json", []byte(`{"clients":[
		{"uri":"spiffe://clients/player","read":["players"],"write":["players"],"allow_head":true},
		{"uri":"spiffe://clients/reader","read":["players"]}
	]}`)))
	settings, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &testService{unaryActive: make(chan struct{}), streamActive: make(chan struct{})}
	server := grpc.NewServer(settings.ServerOptions()...)
	kv.RegisterKVStoreServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return listener.Addr().String(), func(uri string, untrusted bool) *tls.Config {
		config := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		if uri != "" {
			parent, parentKey := ca, caKey
			if untrusted {
				parent, parentKey = wrong, wrongKey
			}
			cert, key, _ := newCertificate(t, parent, parentKey, 4, uri)
			config.Certificates = []tls.Certificate{{Certificate: [][]byte{cert.Raw}, PrivateKey: key}}
		}
		return config
	}, service
}

func connect(t *testing.T, target string, config *tls.Config) kv.KVStoreClient {
	t.Helper()
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(credentials.NewTLS(config)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return kv.NewKVStoreClient(connection)
}

func newCertificate(t *testing.T, parent *x509.Certificate, parentKey ed25519.PrivateKey, serial int64, uri string) (*x509.Certificate, ed25519.PrivateKey, []byte) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "KVStore TLS test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	switch {
	case parent == nil:
		template.IsCA = true
		template.BasicConstraintsValid = true
		template.KeyUsage |= x509.KeyUsageCertSign
		parent, parentKey = template, private
	case uri == "":
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	default:
		parsed, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{parsed}
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, public, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, private, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
