package apikeys

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentCreatesAndRevocation(t *testing.T) {
	dir := t.TempDir()
	const users = 12
	var group sync.WaitGroup
	result := make(chan string, users)
	for range users {
		group.Go(func() {
			key, err := Create(dir, Key{Name: "Shared name", Read: true, Namespaces: []string{"players"}})
			if err == nil {
				result <- key
			} else if !strings.Contains(err.Error(), "already exists") {
				t.Errorf("create error: %v", err)
			}
		})
	}
	group.Wait()
	close(result)
	if len(result) != 1 {
		t.Fatalf("published %d keys with the same name, want 1", len(result))
	}
	secret := <-result
	digest, valid := Digest(secret)
	if !valid {
		t.Fatal("created token does not match the supported format")
	}
	raw, err := os.ReadFile(filepath.Join(dir, filename("Shared name")))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(secret)) {
		t.Fatal("record persisted the key in cleartext")
	}
	loaded, err := Load(dir)
	if err != nil || len(loaded) != 1 || !loaded[digest].Read || loaded[digest].Write {
		t.Fatalf("incorrect loaded grants: %v", err)
	}
	if err := Revoke(dir, "Shared name"); err != nil {
		t.Fatal(err)
	}
	replacement, err := Create(dir, Key{Name: "Shared name", Write: true})
	if err != nil || replacement == secret {
		t.Fatalf("recreated credential did not rotate: %v", err)
	}
	keys, err := Load(dir)
	if err != nil || len(keys) != 1 || keys[digest].Name != "" {
		t.Fatalf("revoked grant still appears: %v", err)
	}
}

func TestInvalidDigestFailsClosed(t *testing.T) {
	dir := t.TempDir()
	invalid := `{"name":"bad","read":true,"sha256":"` + strings.Repeat("f", 66) + `"}`
	if err := os.WriteFile(filepath.Join(dir, filename("bad")), []byte(invalid), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("API key record with oversized digest was accepted")
	}
}
