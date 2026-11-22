// Package access configures the public gRPC endpoint's transport and admission.
package access

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/byPixelTV/kvstore/internal/apikeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	dataMethodPrefix    = "/kvstore.v1.KVStore/"
	consoleMethodPrefix = "/kvstore.v1.Console/"
)

type Settings struct {
	tlsConfig        *tls.Config
	policy           namespacePolicy
	apiKeys          *apiKeyStore
	unary, streams   chan struct{}
	HealthListenAddr string
}

func Load() (Settings, error) {
	return load(os.Getenv("KVSTORE_API_KEYS_DIR"))
}

// LoadStandalone enables the persisted registry on subsequent starts even when
// the initializer is no longer supplied.
func LoadStandalone(dataDir string) (Settings, error) {
	token := os.Getenv("KVSTORE_INIT_API_KEY")
	dir := os.Getenv("KVSTORE_API_KEYS_DIR")
	if dir == "" {
		dir = filepath.Join(dataDir, "api-keys")
		if token == "" {
			if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
				dir = ""
			} else if err != nil {
				return Settings{}, fmt.Errorf("inspect standalone API key registry: %w", err)
			}
		}
	}
	if token != "" {
		if err := apikeys.Bootstrap(dir, token); err != nil {
			return Settings{}, fmt.Errorf("initialize KVSTORE_INIT_API_KEY: %w", err)
		}
	}
	return load(dir)
}

func load(apiKeyDir string) (Settings, error) {
	var s Settings
	certFile := os.Getenv("KVSTORE_TLS_CERT_FILE")
	keyFile := os.Getenv("KVSTORE_TLS_KEY_FILE")
	caFile := os.Getenv("KVSTORE_TLS_CLIENT_CA_FILE")
	policyFile := os.Getenv("KVSTORE_ACCESS_POLICY_FILE")
	s.HealthListenAddr = os.Getenv("KVSTORE_HEALTH_LISTEN_ADDR")
	if certFile != "" || keyFile != "" || caFile != "" {
		if certFile == "" || keyFile == "" || (caFile == "" && apiKeyDir == "") {
			return s, errors.New("TLS access requires a server certificate and key plus KVSTORE_TLS_CLIENT_CA_FILE or KVSTORE_API_KEYS_DIR")
		}
		if s.HealthListenAddr == "" {
			return s, errors.New("TLS requires a loopback KVSTORE_HEALTH_LISTEN_ADDR for local probes")
		}
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return s, fmt.Errorf("load gRPC TLS certificate: %w", err)
		}
		s.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
		}
		if caFile != "" {
			pem, err := os.ReadFile(caFile)
			if err != nil {
				return s, fmt.Errorf("read gRPC client CA: %w", err)
			}
			clientCAs := x509.NewCertPool()
			if !clientCAs.AppendCertsFromPEM(pem) {
				return s, errors.New("KVSTORE_TLS_CLIENT_CA_FILE contains no certificates")
			}
			s.tlsConfig.ClientCAs = clientCAs
			s.tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			if apiKeyDir != "" {
				s.tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
			}
		}
	}
	if apiKeyDir != "" {
		keys, err := apikeys.Load(apiKeyDir)
		if err != nil {
			return s, fmt.Errorf("load API key directory: %w", err)
		}
		s.apiKeys = &apiKeyStore{dir: apiKeyDir}
		s.apiKeys.replace(keys)
	}
	if policyFile != "" {
		if caFile == "" {
			return s, errors.New("KVSTORE_ACCESS_POLICY_FILE requires KVSTORE_TLS_CLIENT_CA_FILE")
		}
		var err error
		s.policy, err = loadPolicy(policyFile)
		if err != nil {
			return s, fmt.Errorf("load gRPC access policy: %w", err)
		}
	}
	var err error
	s.unary, err = loadLimit("KVSTORE_MAX_INFLIGHT_RPCS")
	if err != nil {
		return s, err
	}
	s.streams, err = loadLimit("KVSTORE_MAX_STREAMS")
	return s, err
}

func (s Settings) UsesTLS() bool { return s.tlsConfig != nil }

func (s Settings) UsesClientAuthentication() bool { return s.UsesTLS() || s.apiKeys != nil }

func loadLimit(name string) (chan struct{}, error) {
	value := os.Getenv(name)
	if value == "" || value == "0" {
		return nil, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > 100_000 {
		return nil, fmt.Errorf("%s must be between 0 and 100000", name)
	}
	return make(chan struct{}, limit), nil
}

func (s Settings) ServerOptions() []grpc.ServerOption {
	var options []grpc.ServerOption
	if s.UsesTLS() {
		options = append(options, grpc.Creds(credentials.NewTLS(s.tlsConfig)))
	}
	if s.policy != nil || s.apiKeys != nil || s.unary != nil {
		options = append(options, grpc.ChainUnaryInterceptor(s.interceptUnary))
	}
	if s.policy != nil || s.apiKeys != nil || s.streams != nil {
		options = append(options, grpc.ChainStreamInterceptor(s.interceptStream))
	}
	return options
}

func (s Settings) interceptUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	switch {
	case strings.HasPrefix(info.FullMethod, dataMethodPrefix):
		grants, _, err := s.authorize(ctx)
		if err != nil {
			return nil, err
		}
		if grants != nil && !grants.allows(req) {
			return nil, denied()
		}
	case strings.HasPrefix(info.FullMethod, consoleMethodPrefix):
		// Console methods verify their own session and management grants.
	default:
		return handler(ctx, req)
	}
	if s.unary != nil {
		select {
		case s.unary <- struct{}{}:
			defer func() { <-s.unary }()
		default:
			return nil, status.Error(codes.ResourceExhausted, "gRPC request limit reached before admission")
		}
	}
	return handler(ctx, req)
}

func (s Settings) interceptStream(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !strings.HasPrefix(info.FullMethod, dataMethodPrefix) {
		return handler(srv, stream)
	}
	grants, key, err := s.authorize(stream.Context())
	if err != nil {
		return err
	}
	ctx := stream.Context()
	if key != nil {
		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(ctx)
		stop := context.AfterFunc(key.lifetime, func() { cancel(errAPIKeyRevoked) })
		defer stop()
		defer cancel(nil)
	}
	if grants != nil {
		stream = &authorizedStream{ServerStream: stream, ctx: ctx, grants: *grants, key: key}
	}
	if s.streams != nil {
		select {
		case s.streams <- struct{}{}:
			defer func() { <-s.streams }()
		default:
			return status.Error(codes.ResourceExhausted, "gRPC stream limit reached before admission")
		}
	}
	err = handler(srv, stream)
	if errors.Is(context.Cause(ctx), errAPIKeyRevoked) {
		return unauthenticated()
	}
	return err
}
