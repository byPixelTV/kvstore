package access

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type namespacePolicy map[string]namespaceGrants

type namespaceGrants struct {
	read, write       map[string]bool
	readAll, writeAll bool
	head              bool
}

func loadPolicy(path string) (namespacePolicy, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var document struct {
		Clients []struct {
			URI       string   `json:"uri"`
			Read      []string `json:"read"`
			Write     []string `json:"write"`
			AllowHead bool     `json:"allow_head"`
		} `json:"clients"`
	}
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("expected a single policy document")
	}
	if len(document.Clients) == 0 {
		return nil, errors.New("policy requires at least one client grant")
	}
	policy := make(namespacePolicy, len(document.Clients))
	for i, client := range document.Clients {
		uri, err := url.Parse(client.URI)
		if err != nil || !uri.IsAbs() || uri.String() != client.URI || uri.Fragment != "" {
			return nil, fmt.Errorf("client %d requires a valid absolute URI SAN", i+1)
		}
		if _, exists := policy[client.URI]; exists {
			return nil, fmt.Errorf("client %d duplicates a URI SAN", i+1)
		}
		read, err := namespaceSet(client.Read)
		if err != nil {
			return nil, fmt.Errorf("client %d read grant: %w", i+1, err)
		}
		write, err := namespaceSet(client.Write)
		if err != nil {
			return nil, fmt.Errorf("client %d write grant: %w", i+1, err)
		}
		policy[client.URI] = namespaceGrants{read: read, write: write, head: client.AllowHead}
	}
	return policy, nil
}

func namespaceSet(namespaces []string) (map[string]bool, error) {
	out := make(map[string]bool, len(namespaces))
	for _, namespace := range namespaces {
		if namespace == "" || out[namespace] {
			return nil, errors.New("namespaces must be nonempty and unique")
		}
		out[namespace] = true
	}
	return out, nil
}

func verifiedCertificate(ctx context.Context) *x509.Certificate {
	client, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}
	identity, ok := client.AuthInfo.(credentials.TLSInfo)
	if !ok || len(identity.State.VerifiedChains) == 0 || len(identity.State.VerifiedChains[0]) == 0 {
		return nil
	}
	return identity.State.VerifiedChains[0][0]
}

func (p namespacePolicy) grants(ctx context.Context) (namespaceGrants, error) {
	leaf := verifiedCertificate(ctx)
	if leaf == nil || len(leaf.URIs) != 1 {
		return namespaceGrants{}, denied()
	}
	grants, ok := p[leaf.URIs[0].String()]
	if !ok {
		return namespaceGrants{}, denied()
	}
	return grants, nil
}

func (g namespaceGrants) canRead(namespace string) bool { return g.readAll || g.read[namespace] }

func (g namespaceGrants) canWrite(namespace string) bool { return g.writeAll || g.write[namespace] }

func (g namespaceGrants) hasNamespaceAccess() bool {
	return g.readAll || g.writeAll || len(g.read) != 0 || len(g.write) != 0
}

func (g namespaceGrants) allows(req any) bool {
	switch req := req.(type) {
	case *kvstorev1.GetRequest:
		return g.canRead(req.GetNamespace())
	case *kvstorev1.SetRequest:
		return g.canWrite(req.GetNamespace())
	case *kvstorev1.DeleteRequest:
		return g.canWrite(req.GetNamespace())
	case *kvstorev1.BatchWriteRequest:
		if !g.writeAll && len(g.write) == 0 {
			return false
		}
		for _, op := range req.GetOperations() {
			if !g.canWrite(op.GetNamespace()) {
				return false
			}
		}
		return true
	case *kvstorev1.HeadRequest:
		return g.head
	case *kvstorev1.ScanRequest:
		return g.canRead(req.GetNamespace())
	case *kvstorev1.WatchRequest:
		return g.canRead(req.GetNamespace())
	default:
		return false
	}
}

type authorizedStream struct {
	grpc.ServerStream
	ctx    context.Context
	grants namespaceGrants
	key    *activeAPIKey
}

func (s *authorizedStream) Context() context.Context { return s.ctx }

func (s *authorizedStream) RecvMsg(req any) error {
	if err := s.ServerStream.RecvMsg(req); err != nil {
		return err
	}
	if s.key != nil && s.key.lifetime.Err() != nil {
		return unauthenticated()
	}
	if !s.grants.allows(req) {
		return denied()
	}
	return nil
}

func (s *authorizedStream) SendMsg(msg any) error {
	if s.key != nil && s.key.lifetime.Err() != nil {
		return unauthenticated()
	}
	return s.ServerStream.SendMsg(msg)
}

func denied() error { return status.Error(codes.PermissionDenied, "gRPC access policy denied request") }
