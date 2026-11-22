package access

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/byPixelTV/kvstore/internal/apikeys"
)

func TestStandaloneInitialAPIKey(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "api-keys")
	t.Setenv("KVSTORE_API_KEYS_DIR", dir)
	t.Setenv("KVSTORE_INIT_API_KEY", "")
	if _, err := LoadStandalone(data); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing registry must fail closed: %v", err)
	}
	t.Setenv("KVSTORE_INIT_API_KEY", "invalid")
	if _, err := LoadStandalone(data); !errors.Is(err, apikeys.ErrInvalidKey) {
		t.Fatalf("invalid initializer must fail: %v", err)
	}
	token := createTestKey(t, t.TempDir(), apikeys.Key{Name: "fixture", Read: true})
	digest, _ := apikeys.Digest(token)
	t.Setenv("KVSTORE_INIT_API_KEY", token)
	settings, err := LoadStandalone(data)
	if err != nil {
		t.Fatal(err)
	}
	granted := (*settings.apiKeys.current.Load())[digest]
	if granted == nil || granted.key.Name != "bootstrap" || !granted.key.Manage ||
		!granted.grants.readAll || !granted.grants.writeAll {
		t.Fatal("initializer did not create a management credential")
	}

	t.Setenv("KVSTORE_API_KEYS_DIR", "")
	t.Setenv("KVSTORE_INIT_API_KEY", "")
	settings, err = LoadStandalone(data)
	if err != nil {
		t.Fatal(err)
	}
	if (*settings.apiKeys.current.Load())[digest] == nil {
		t.Fatal("persisted key unavailable without the initializer")
	}
	if err := apikeys.Revoke(dir, "bootstrap"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KVSTORE_INIT_API_KEY", token)
	settings, err = LoadStandalone(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(*settings.apiKeys.current.Load()) != 0 {
		t.Fatal("restart restored a revoked bootstrap credential")
	}
}
