// Package console connects terminal clients to the KVStore data and console APIs.
package console

import (
	"context"
	"time"

	"github.com/byPixelTV/kvstore/internal/clienttls"
)

type Endpoint struct {
	Address   string
	Transport clienttls.Options
}

type AccessKey struct {
	Name                string
	Read, Write, Manage bool
	Namespaces          []string
}

type Record struct {
	Namespace string
	Key       string
	Value     []byte
	Revision  uint64
	ExpiresAt time.Time
}

type ScanResult struct {
	Records []Record
	More    bool
}

type Mutation struct {
	Namespace string
	Key       string
	Value     []byte
	ExpiresAt time.Time
	Delete    bool
}

type MutationResult struct {
	Revision uint64
	Deleted  bool
	Error    string
}

type Head struct {
	Revision           uint64
	OldestRevision     uint64
	Cursor             string
	SupportsExpiration bool
}

type Event struct {
	Record     Record
	Deleted    bool
	Checkpoint bool
}

type WatchStream interface {
	Next() (Event, error)
}

type Client interface {
	Close() error
	Namespaces(context.Context) ([]string, error)
	Scan(context.Context, string, int) (ScanResult, error)
	Get(context.Context, string, string) (Record, bool, error)
	Set(context.Context, Record) (Record, error)
	Delete(context.Context, string, string) (bool, error)
	Batch(context.Context, []Mutation) ([]MutationResult, error)
	Head(context.Context) (Head, error)
	Watch(context.Context, string, string) (WatchStream, error)
	ListKeys(context.Context) ([]AccessKey, error)
	CreateKey(context.Context, AccessKey) (string, error)
	RevokeKey(context.Context, string) error
}

type Connector func(context.Context, Endpoint, string) (Client, AccessKey, error)
