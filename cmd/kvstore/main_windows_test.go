package main

import (
	"strings"
	"testing"
)

func TestRunRejectsNativeWindowsRaft(t *testing.T) {
	t.Setenv("KVSTORE_MODE", "cluster")

	err := run()
	if err == nil || !strings.Contains(err.Error(), "Raft voters are unsupported on native Windows") {
		t.Fatalf("expected a Windows Raft support error, got %v", err)
	}
}
