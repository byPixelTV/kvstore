package apikeys

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Bootstrap creates a new registry containing the initial management key.
// An existing registry is authoritative, even if the initial key was revoked.
func Bootstrap(dir, token string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	digest, ok := Digest(token)
	if !ok {
		return fmt.Errorf("%w: expected a kvs_ token containing 32 base64url-encoded bytes", ErrInvalidKey)
	}
	parent := filepath.Dir(dir)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(parent, ".api-keys-")
	if err != nil {
		return err
	}
	key := Key{Name: "bootstrap", Read: true, Write: true, Manage: true}
	defer os.Remove(staging)
	defer os.Remove(filepath.Join(staging, filename(key.Name)))
	if err := create(staging, key, digest); err != nil {
		return err
	}
	if err := os.Rename(staging, dir); err != nil {
		// A concurrent initializer may have installed its complete registry.
		if _, statErr := os.Stat(dir); statErr == nil && (errors.Is(err, os.ErrExist) || errors.Is(err, os.ErrPermission)) {
			return nil
		}
		return err
	}
	return syncDirectory(parent)
}
