package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	raftwal "github.com/hashicorp/raft-wal"
)

// openRaftWAL is the only supported persistent Raft LogStore/StableStore.
// Old BoltDB state is intentionally not opened, migrated or deleted: mixing
// log formats with a retained Pebble FSM risks silently diverging state.
func openRaftWAL(raftDir string) (*raftwal.WAL, error) {
	legacy := filepath.Join(raftDir, "raft.db")
	if _, err := os.Stat(legacy); err == nil {
		return nil, fmt.Errorf("legacy Raft BoltDB found at %s: KVStore now requires raft-wal; reset the entire DEVELOPMENT node data directory (including Pebble state), not just raft.db", legacy)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("check legacy Raft storage: %w", err)
	}

	walDir := filepath.Join(raftDir, "wal")
	if err := os.MkdirAll(walDir, 0700); err != nil {
		return nil, fmt.Errorf("create Raft WAL directory: %w", err)
	}
	store, err := raftwal.Open(walDir)
	if err != nil {
		return nil, fmt.Errorf("open Raft WAL: %w", err)
	}
	return store, nil
}
