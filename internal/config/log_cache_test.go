package config

import "testing"

func TestRaftLogCacheBounds(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "standalone")
	t.Setenv("KVSTORE_RAFT_LOG_CACHE_ENTRIES", "512")
	c, err := Load()
	if err != nil || c.RaftLogCacheEntries != 512 {
		t.Fatalf("invalid cache config: %+v %v", c, err)
	}
	t.Setenv("KVSTORE_RAFT_LOG_CACHE_ENTRIES", "0")
	if c, err := Load(); err != nil || c.RaftLogCacheEntries != 0 {
		t.Fatalf("cache disable failed: %+v %v", c, err)
	}
	t.Setenv("KVSTORE_RAFT_LOG_CACHE_ENTRIES", "8193")
	if _, err := Load(); err == nil {
		t.Fatal("invalid cache size was accepted")
	}
}
