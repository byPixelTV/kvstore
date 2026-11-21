// Package access configures the public gRPC endpoint's transport and admission.
package access

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const dataMethodPrefix = "/kvstore.v1.KVStore/"

type Settings struct {
	tlsConfig        *tls.Config
	policy           namespacePolicy
	unary, streams   chan struct{}
	HealthListenAddr string
}

func Load() (Settings, error) {
	var s Settings
	certFile := os.Getenv("KVSTORE_TLS_CERT_FILE")
	keyFile := os.Getenv("KVSTORE_TLS_KEY_FILE")
	caFile := os.Getenv("KVSTORE_TLS_CLIENT_CA_FILE")
	policyFile := os.Getenv("KVSTORE_ACCESS_POLICY_FILE")
	s.HealthListenAddr = os.Getenv("KVSTORE_HEALTH_LISTEN_ADDR")
	if certFile != "" || keyFile != "" || caFile != "" {
		if certFile == "" || keyFile == "" || caFile == "" {
			return s, errors.New("mTLS requires KVSTORE_TLS_CERT_FILE, KVSTORE_TLS_KEY_FILE and KVSTORE_TLS_CLIENT_CA_FILE")
		}
		if s.HealthListenAddr == "" {
			return s, errors.New("mTLS requires a loopback KVSTORE_HEALTH_LISTEN_ADDR for local probes")
		}
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return s, fmt.Errorf("load gRPC TLS certificate: %w", err)
		}
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return s, fmt.Errorf("read gRPC client CA: %w", err)
		}
		clientCAs := x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(pem) {
			return s, errors.New("KVSTORE_TLS_CLIENT_CA_FILE contains no certificates")
		}
		s.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{certificate}, ClientCAs: clientCAs,
			ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
		}
	}
	if policyFile != "" {
		if !s.UsesTLS() {
			return s, errors.New("KVSTORE_ACCESS_POLICY_FILE requires gRPC mTLS")
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
	if s.policy != nil || s.unary != nil {
		options = append(options, grpc.ChainUnaryInterceptor(s.interceptUnary))
	}
	if s.policy != nil || s.streams != nil {
		options = append(options, grpc.ChainStreamInterceptor(s.interceptStream))
	}
	return options
}

func (s Settings) interceptUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !strings.HasPrefix(info.FullMethod, dataMethodPrefix) {
		return handler(ctx, req)
	}
	if s.policy != nil {
		grants, err := s.policy.grants(ctx)
		if err != nil {
			return nil, err
		}
		if !grants.allows(req) {
			return nil, denied()
		}
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
	if s.policy != nil {
		grants, err := s.policy.grants(stream.Context())
		if err != nil {
			return err
		}
		stream = &authorizedStream{ServerStream: stream, grants: grants}
	}
	if s.streams != nil {
		select {
		case s.streams <- struct{}{}:
			defer func() { <-s.streams }()
		default:
			return status.Error(codes.ResourceExhausted, "gRPC stream limit reached before admission")
		}
	}
	return handler(srv, stream)
}
