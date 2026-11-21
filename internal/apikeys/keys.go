// Package apikeys manages named credentials for a KVStore endpoint.
package apikeys

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const tokenPrefix = "kvs_"
const secretBytes = 32
const maxRecordBytes = 64 << 10

var (
	ErrInvalidKey          = errors.New("invalid API key settings")
	ErrKeyExists           = errors.New("API key already exists")
	ErrKeyNotFound         = errors.New("API key does not exist")
	ErrUncertainDurability = errors.New("could not sync API key registry directory")
)

type Key struct {
	Name       string   `json:"name"`
	Read       bool     `json:"read"`
	Write      bool     `json:"write"`
	Manage     bool     `json:"manage,omitempty"`
	Namespaces []string `json:"namespaces,omitempty"`
}

type record struct {
	Key
	SHA256 string `json:"sha256"`
}

func validateName(name string) error {
	if name == "" || len(name) > 80 || !utf8.ValidString(name) || strings.TrimSpace(name) != name || strings.ContainsFunc(name, unicode.IsControl) {
		return fmt.Errorf("%w: key name must be 1–80 bytes without surrounding spaces or control characters", ErrInvalidKey)
	}
	return nil
}

func (k Key) validate() error {
	if err := validateName(k.Name); err != nil {
		return err
	}
	if !k.Read && !k.Write && !k.Manage {
		return fmt.Errorf("%w: grant at least one of read, write, or manage", ErrInvalidKey)
	}
	seen := make(map[string]bool, len(k.Namespaces))
	for _, ns := range k.Namespaces {
		if ns == "" || !utf8.ValidString(ns) || seen[ns] {
			return fmt.Errorf("%w: namespaces must be nonempty, unique UTF-8 strings", ErrInvalidKey)
		}
		seen[ns] = true
	}
	return nil
}

func filename(name string) string {
	digest := sha256.Sum256([]byte(name))
	return hex.EncodeToString(digest[:]) + ".json"
}

// Create returns the secret once. A nonempty secret and a directory-sync
// error mean the key was published with uncertain crash durability.
func Create(dir string, key Key) (secret string, err error) {
	if err := key.validate(); err != nil {
		return "", err
	}
	var random [secretBytes]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	token := tokenPrefix + base64.RawURLEncoding.EncodeToString(random[:])
	digest := sha256.Sum256([]byte(token))
	payload, err := json.Marshal(record{Key: key, SHA256: hex.EncodeToString(digest[:])})
	if err != nil {
		return "", err
	}
	if len(payload) > maxRecordBytes {
		return "", fmt.Errorf("%w: record exceeds 64 KiB", ErrInvalidKey)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, ".key-")
	if err != nil {
		return "", err
	}
	defer os.Remove(f.Name())
	writeErr := writeRecord(f, payload)
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return "", err
	}
	// Link publishes a complete file without replacing a concurrently created key.
	if err := os.Link(f.Name(), filepath.Join(dir, filename(key.Name))); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%w: %q", ErrKeyExists, key.Name)
		}
		return "", err
	}
	return token, syncDirectory(dir)
}

func writeRecord(f *os.File, payload []byte) error {
	if _, err := f.Write(payload); err != nil {
		return err
	}
	if err := f.Chmod(0640); err != nil {
		return err
	}
	return f.Sync()
}

func Revoke(dir, name string) error {
	if err := validateName(name); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, filename(name))); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %q", ErrKeyNotFound, name)
		}
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUncertainDurability, err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return fmt.Errorf("%w: %w", ErrUncertainDurability, err)
	}
	return nil
}

func List(dir string) ([]Key, error) {
	keys, err := Load(dir)
	if err != nil {
		return nil, err
	}
	out := make([]Key, 0, len(keys))
	for _, key := range keys {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Digest checks the fixed token format before hashing it for lookup.
func Digest(token string) ([sha256.Size]byte, bool) {
	encoded, ok := strings.CutPrefix(token, tokenPrefix)
	if !ok || len(encoded) != base64.RawURLEncoding.EncodedLen(secretBytes) {
		return [sha256.Size]byte{}, false
	}
	var secret [secretBytes]byte
	n, err := base64.RawURLEncoding.Strict().Decode(secret[:], []byte(encoded))
	if err != nil || n != secretBytes {
		return [sha256.Size]byte{}, false
	}
	return sha256.Sum256([]byte(token)), true
}

func Load(dir string) (map[[sha256.Size]byte]Key, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	keys := make(map[[sha256.Size]byte]Key, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		rec, err := readRecord(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue // Concurrent revoke or projected volume update.
		}
		if err != nil {
			return nil, fmt.Errorf("API key record %s: %w", name, err)
		}
		if name != filename(rec.Name) {
			return nil, fmt.Errorf("API key record %s has a mismatched name", name)
		}
		var digest [sha256.Size]byte
		if len(rec.SHA256) != hex.EncodedLen(len(digest)) {
			return nil, fmt.Errorf("API key record %s has an invalid digest", name)
		}
		n, err := hex.Decode(digest[:], []byte(rec.SHA256))
		if err != nil || n != len(digest) {
			return nil, fmt.Errorf("API key record %s has an invalid digest", name)
		}
		if _, exists := keys[digest]; exists {
			return nil, errors.New("API key directory contains duplicate digests")
		}
		keys[digest] = rec.Key
	}
	return keys, nil
}

func readRecord(path string) (record, error) {
	var rec record
	info, err := os.Stat(path)
	if err != nil {
		return rec, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxRecordBytes {
		return rec, errors.New("expected a regular API key file up to 64 KiB")
	}
	f, err := os.Open(path)
	if err != nil {
		return rec, err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, maxRecordBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&rec); err != nil {
		return rec, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return rec, errors.New("expected a single API key record")
	}
	return rec, rec.validate()
}
