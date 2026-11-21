package access

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"slices"
	"strings"
	"syscall"

	kvstorev1 "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/apikeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// RegisterConsole adds namespace discovery and API key management. Namespace
// discovery uses the same transport and namespace grants as the data service.
func (s Settings) RegisterConsole(server *grpc.Server, list func(context.Context) ([]string, error)) {
	kvstorev1.RegisterConsoleServer(server, &consoleService{settings: s, listNamespaces: list})
}

type consoleService struct {
	kvstorev1.UnimplementedConsoleServer
	settings       Settings
	listNamespaces func(context.Context) ([]string, error)
}

func (s *consoleService) GetSession(ctx context.Context, _ *kvstorev1.GetSessionRequest) (*kvstorev1.GetSessionResponse, error) {
	key, err := s.settings.apiKeys.authenticateConsole(metadata.ValueFromIncomingContext(ctx, "authorization"))
	if err != nil {
		return nil, err
	}
	return &kvstorev1.GetSessionResponse{Key: accessKeyMessage(key.key)}, nil
}

func (s *consoleService) ListNamespaces(ctx context.Context, _ *kvstorev1.ListNamespacesRequest) (*kvstorev1.ListNamespacesResponse, error) {
	grants, _, err := s.settings.authorizeWith(ctx, s.settings.apiKeys.authenticateConsole)
	if err != nil {
		return nil, err
	}
	if grants != nil && !grants.hasNamespaceAccess() {
		return nil, denied()
	}
	available, err := s.listNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(available))
	for _, name := range available {
		if grants == nil || grants.canRead(name) || grants.canWrite(name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return &kvstorev1.ListNamespacesResponse{Namespaces: slices.Compact(names)}, nil
}

func (s *consoleService) ListAccessKeys(ctx context.Context, _ *kvstorev1.ListAccessKeysRequest) (*kvstorev1.ListAccessKeysResponse, error) {
	registry := s.settings.apiKeys
	response := &kvstorev1.ListAccessKeysResponse{}
	err := registry.withManager(ctx, func() error {
		snapshot := *registry.current.Load()
		response.Keys = make([]*kvstorev1.AccessKey, 0, len(snapshot))
		for _, key := range snapshot {
			response.Keys = append(response.Keys, accessKeyMessage(key.key))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(response.Keys, func(a, b *kvstorev1.AccessKey) int {
		return strings.Compare(a.GetName(), b.GetName())
	})
	return response, nil
}

func (s *consoleService) CreateAccessKey(ctx context.Context, request *kvstorev1.CreateAccessKeyRequest) (*kvstorev1.CreateAccessKeyResponse, error) {
	registry := s.settings.apiKeys
	var secret string
	err := registry.withManager(ctx, func() error {
		input := request.GetKey()
		var err error
		secret, err = apikeys.Create(registry.dir, apikeys.Key{
			Name: input.GetName(), Read: input.GetRead(), Write: input.GetWrite(),
			Manage: input.GetManage(), Namespaces: input.GetNamespaces(),
		})
		if secret != "" {
			_ = registry.reloadLocked()
		}
		return registryMutationError(err)
	})
	if err != nil {
		return nil, err
	}
	return &kvstorev1.CreateAccessKeyResponse{Secret: secret}, nil
}

func (s *consoleService) RevokeAccessKey(ctx context.Context, request *kvstorev1.RevokeAccessKeyRequest) (*kvstorev1.RevokeAccessKeyResponse, error) {
	registry := s.settings.apiKeys
	err := registry.withManager(ctx, func() error {
		err := apikeys.Revoke(registry.dir, request.GetName())
		if err == nil || errors.Is(err, apikeys.ErrUncertainDurability) {
			_ = registry.reloadLocked()
		}
		return registryMutationError(err)
	})
	if err != nil {
		return nil, err
	}
	return &kvstorev1.RevokeAccessKeyResponse{}, nil
}

func (s *apiKeyStore) withManager(ctx context.Context, call func() error) error {
	if s == nil {
		return apiKeyRequired()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, err := s.authenticateConsoleLocked(metadata.ValueFromIncomingContext(ctx, "authorization"))
	if err != nil {
		return err
	}
	if !key.key.Manage {
		return denied()
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return call()
}

func accessKeyMessage(key apikeys.Key) *kvstorev1.AccessKey {
	return &kvstorev1.AccessKey{
		Name: key.Name, Read: key.Read, Write: key.Write, Manage: key.Manage,
		Namespaces: slices.Clone(key.Namespaces),
	}
}

func registryMutationError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, apikeys.ErrUncertainDurability):
		// Publication already succeeded. In particular, report a created key's
		// secret while it is still available.
		slog.Warn("API key change applied without confirmed crash durability")
		return nil
	case errors.Is(err, apikeys.ErrInvalidKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, apikeys.ErrKeyExists):
		return status.Error(codes.AlreadyExists, "API key name already exists")
	case errors.Is(err, apikeys.ErrKeyNotFound):
		return status.Error(codes.NotFound, "API key name does not exist")
	case errors.Is(err, fs.ErrPermission), errors.Is(err, syscall.EROFS):
		return status.Error(codes.FailedPrecondition, "API key registry is read-only or lacks write permission")
	default:
		return status.Error(codes.Internal, "could not write the API key registry")
	}
}

func apiKeyRequired() error { return status.Error(codes.Unauthenticated, "valid API key required") }
