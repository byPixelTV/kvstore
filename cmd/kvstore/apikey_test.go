package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/byPixelTV/kvstore/internal/apikeys"
)

func TestKeyManagement(t *testing.T) {
	t.Setenv("KVSTORE_API_KEYS_DIR", t.TempDir())
	var out bytes.Buffer
	if err := runAPIKey([]string{"create", "--name", "invalid"}, &out, io.Discard); err == nil || out.Len() != 0 {
		t.Fatal("command accepted a key without permissions")
	}
	if err := runAPIKey([]string{"create", "--name", "game writer", "--write", "--namespace", "players"}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	secret := strings.TrimSpace(out.String())
	if _, ok := apikeys.Digest(secret); !ok {
		t.Fatal("create did not print a valid secret")
	}
	out.Reset()
	if err := runAPIKey([]string{"list"}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), secret) || strings.Contains(out.String(), "sha256") {
		t.Fatal("list disclosed credential material")
	}
	var keys []apikeys.Key
	if err := json.Unmarshal(out.Bytes(), &keys); err != nil || len(keys) != 1 || keys[0].Name != "game writer" ||
		keys[0].Read || !keys[0].Write || len(keys[0].Namespaces) != 1 || keys[0].Namespaces[0] != "players" {
		t.Fatalf("unexpected list output: %v", err)
	}
	if err := runAPIKey([]string{"revoke", "--name", "game writer"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := runAPIKey([]string{"list"}, &out, io.Discard); err != nil || out.String() != "[]\n" {
		t.Fatalf("revoked key not removed: %v", err)
	}
}

func TestAPIKeyLocalBootstrap(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KVSTORE_API_KEYS_DIR", dir)
	var out bytes.Buffer
	err := runAPIKey([]string{"create", "--manage", "--read", "--write", "--name", "owner"}, &out, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	digest, valid := apikeys.Digest(strings.TrimSpace(out.String()))
	if !valid {
		t.Fatal("creation did not print a valid secret")
	}
	keys, err := apikeys.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := keys[digest]
	if key.Name != "owner" || !key.Manage || !key.Read || !key.Write || len(key.Namespaces) != 0 {
		t.Fatalf("unexpected grants for the first operator: %+v", key)
	}
}
