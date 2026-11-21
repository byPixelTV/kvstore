package access

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/byPixelTV/kvstore/internal/apikeys"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type activeAPIKey struct {
	key      apikeys.Key
	grants   namespaceGrants
	lifetime context.Context
	cancel   context.CancelFunc
}

type apiKeySnapshot map[[sha256.Size]byte]*activeAPIKey

type apiKeyStore struct {
	dir     string
	mu      sync.Mutex // Serializes directory refreshes and console mutations.
	current atomic.Pointer[apiKeySnapshot]
}

func (s *apiKeyStore) replace(keys map[[sha256.Size]byte]apikeys.Key) {
	var previous apiKeySnapshot
	if snapshot := s.current.Load(); snapshot != nil {
		previous = *snapshot
	}
	next := make(apiKeySnapshot, len(keys))
	for digest, key := range keys {
		if old := previous[digest]; old != nil && old.key.Name == key.Name && old.key.Read == key.Read &&
			old.key.Write == key.Write && old.key.Manage == key.Manage && slices.Equal(old.key.Namespaces, key.Namespaces) {
			next[digest] = old
			continue
		}
		namespaces := make(map[string]bool, len(key.Namespaces))
		for _, ns := range key.Namespaces {
			namespaces[ns] = true
		}
		grants := namespaceGrants{head: key.Read}
		if key.Read {
			grants.read = namespaces
			grants.readAll = len(namespaces) == 0
		}
		if key.Write {
			grants.write = namespaces
			grants.writeAll = len(namespaces) == 0
		}
		lifetime, cancel := context.WithCancel(context.Background())
		next[digest] = &activeAPIKey{key: key, grants: grants, lifetime: lifetime, cancel: cancel}
	}
	s.current.Store(&next)
	for digest, old := range previous {
		if next[digest] != old {
			old.cancel()
		}
	}
}

func (s *apiKeyStore) reloadLocked() error {
	keys, err := apikeys.Load(s.dir)
	s.replace(keys)
	return err
}

// WatchAPIKeys reloads the registry for endpoints during their serving lifetime.
// A missing or invalid registry immediately disables API key authentication.
func (s Settings) WatchAPIKeys(ctx context.Context) {
	if s.apiKeys == nil {
		return
	}
	failed := false
	reload := func() {
		s.apiKeys.mu.Lock()
		err := s.apiKeys.reloadLocked()
		s.apiKeys.mu.Unlock()
		if err != nil && !failed {
			slog.Warn("API keys disabled until the registry is readable", "error", err)
		}
		failed = err != nil
	}
	reload()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		defer func() {
			s.apiKeys.mu.Lock()
			defer s.apiKeys.mu.Unlock()
			s.apiKeys.replace(nil)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reload()
			}
		}
	}()
}

func bearerDigest(values []string) ([sha256.Size]byte, bool) {
	if len(values) != 1 {
		return [sha256.Size]byte{}, false
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return [sha256.Size]byte{}, false
	}
	return apikeys.Digest(token)
}

func (s *apiKeyStore) lookup(digest [sha256.Size]byte) *activeAPIKey {
	snapshot := s.current.Load()
	if snapshot == nil {
		return nil
	}
	key := (*snapshot)[digest]
	if key == nil || key.lifetime.Err() != nil {
		return nil
	}
	return key
}

func (s *apiKeyStore) authenticate(values []string) (*activeAPIKey, error) {
	if digest, ok := bearerDigest(values); ok {
		if key := s.lookup(digest); key != nil {
			return key, nil
		}
	}
	return nil, unauthenticated()
}

func (s *apiKeyStore) authenticateConsole(values []string) (*activeAPIKey, error) {
	if s == nil {
		return nil, apiKeyRequired()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.authenticateConsoleLocked(values)
}

// Console authorization consults disk so a revoked manager cannot use a stale
// serving snapshot. Callers that mutate the registry keep the write lock held.
func (s *apiKeyStore) authenticateConsoleLocked(values []string) (*activeAPIKey, error) {
	digest, ok := bearerDigest(values)
	if !ok || s.reloadLocked() != nil {
		return nil, apiKeyRequired()
	}
	key := s.lookup(digest)
	if key == nil {
		return nil, apiKeyRequired()
	}
	return key, nil
}

// authorize separates identity selection from per-request permission checks.
func (s Settings) authorize(ctx context.Context) (grants *namespaceGrants, key *activeAPIKey, err error) {
	return s.authorizeWith(ctx, s.apiKeys.authenticate)
}

func (s Settings) authorizeWith(ctx context.Context, authenticate func([]string) (*activeAPIKey, error)) (grants *namespaceGrants, key *activeAPIKey, err error) {
	if s.apiKeys != nil {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		if len(values) != 0 {
			key, err := authenticate(values)
			if err != nil {
				return nil, nil, err
			}
			return &key.grants, key, nil
		}
		if verifiedCertificate(ctx) == nil {
			return nil, nil, unauthenticated()
		}
	}
	if s.policy != nil {
		grants, err := s.policy.grants(ctx)
		return &grants, nil, err
	}
	return nil, nil, nil
}

var errAPIKeyRevoked = errors.New("API key revoked")

func unauthenticated() error {
	return status.Error(codes.Unauthenticated, "valid API key or client certificate required")
}
