// Package kvstore is a gRPC client for KVStore.
package kvstore

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Options struct {
	// TLS uses the supplied trust roots and client certificate, if present.
	// Nil uses the system trust roots. Plaintext explicitly disables TLS.
	TLS       *tls.Config
	Plaintext bool
	APIKey    string
}

type Client struct {
	rpc    pb.KVStoreClient
	conn   *grpc.ClientConn
	apiKey string
}

// Dial creates a client that owns its connection. gRPC connects on demand.
func Dial(target string, options Options) (*Client, error) {
	if options.Plaintext && options.TLS != nil {
		return nil, errors.New("kvstore: plaintext and TLS are mutually exclusive")
	}
	transport := insecure.NewCredentials()
	if !options.Plaintext {
		config := options.TLS
		if config == nil {
			config = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		transport = credentials.NewTLS(config)
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(transport))
	if err != nil {
		return nil, err
	}
	client := FromConn(conn, options.APIKey)
	client.conn = conn
	return client, nil
}

// FromConn uses a caller-managed connection, for example one with interceptors.
func FromConn(conn grpc.ClientConnInterface, apiKey string) *Client {
	return &Client{rpc: pb.NewKVStoreClient(conn), apiKey: apiKey}
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) authorized(ctx context.Context) context.Context {
	if c.apiKey == "" {
		return ctx
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	if md == nil {
		md = metadata.MD{}
	}
	md.Set("authorization", "Bearer "+c.apiKey)
	return metadata.NewOutgoingContext(ctx, md)
}

type ReadConsistency = pb.ReadConsistency

const (
	Strong   = pb.ReadConsistency_READ_CONSISTENCY_STRONG
	Eventual = pb.ReadConsistency_READ_CONSISTENCY_EVENTUAL
)

func (c *Client) Get(ctx context.Context, namespace, key string) (*pb.GetResponse, error) {
	return c.Read(ctx, namespace, key, Strong)
}

func (c *Client) Read(ctx context.Context, namespace, key string, consistency ReadConsistency) (*pb.GetResponse, error) {
	return c.rpc.Get(c.authorized(ctx), &pb.GetRequest{Namespace: namespace, Key: key, Consistency: consistency})
}

func (c *Client) Set(ctx context.Context, namespace, key string, value []byte, expiresAtUnixMS int64) (*pb.SetResponse, error) {
	return c.rpc.Set(c.authorized(ctx), &pb.SetRequest{
		Namespace: namespace, Key: key, Value: value, ExpiresAtUnixMs: expiresAtUnixMS,
	})
}

func (c *Client) Delete(ctx context.Context, namespace, key string) (*pb.DeleteResponse, error) {
	return c.rpc.Delete(c.authorized(ctx), &pb.DeleteRequest{Namespace: namespace, Key: key})
}

// BatchWrite returns per-operation outcomes in request order. A transport error
// can leave the outcome unknown. A batch spanning multiple shards is not atomic.
func (c *Client) BatchWrite(ctx context.Context, operations ...*pb.BatchWriteOperation) ([]*pb.BatchWriteResult, error) {
	resp, err := c.rpc.BatchWrite(c.authorized(ctx), &pb.BatchWriteRequest{Operations: operations})
	if err != nil {
		return nil, err
	}
	return resp.Results, nil
}

func GetJSON[T any](ctx context.Context, c *Client, namespace, key string) (value T, found bool, err error) {
	response, err := c.Get(ctx, namespace, key)
	if err != nil || !response.GetFound() {
		return value, false, err
	}
	err = json.Unmarshal(response.Value, &value)
	return value, true, err
}

func SetJSON[T any](ctx context.Context, c *Client, namespace, key string, value T, expiresAtUnixMS int64) (*pb.SetResponse, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return c.Set(ctx, namespace, key, data, expiresAtUnixMS)
}

// Cursor is a resume position scoped to the same endpoint and watch filter.
// In sharded mode only Vector is used.
type Cursor struct {
	Revision uint64
	Vector   string
}

func (c *Client) Head(ctx context.Context) (*pb.HeadResponse, error) {
	return c.rpc.Head(c.authorized(ctx), &pb.HeadRequest{UseVectorCursor: true})
}

func (c *Client) Scan(ctx context.Context, namespace string) (grpc.ServerStreamingClient[pb.ScanResponse], error) {
	return c.rpc.Scan(c.authorized(ctx), &pb.ScanRequest{Namespace: namespace, UseVectorCursor: true})
}

// Watch replays committed changes after the supplied cursor and then waits for
// new ones. With nil it starts at Head. An empty key watches the namespace.
// Cancel ctx to stop. CursorFromChange also accepts checkpoint frames.
func (c *Client) Watch(ctx context.Context, namespace, key string, after *Cursor) (grpc.ServerStreamingClient[pb.WatchResponse], error) {
	if after == nil {
		head, err := c.Head(ctx)
		if err != nil {
			return nil, err
		}
		cursor := CursorFromHead(head)
		after = &cursor
	}
	revision := after.Revision
	if after.Vector != "" {
		revision = 0
	}
	return c.rpc.Watch(c.authorized(ctx), &pb.WatchRequest{
		Namespace: namespace, Key: key, AfterRevision: revision, Cursor: after.Vector, UseVectorCursor: true,
	})
}

func CursorFromHead(head *pb.HeadResponse) Cursor {
	if head.GetCursor() != "" {
		return Cursor{Vector: head.GetCursor()}
	}
	return Cursor{Revision: head.GetRevision()}
}

// CursorFromScan reads the snapshot position from a checkpoint. Callers must
// finish the scan before using it. A sharded scan's checkpoint is at the end.
func CursorFromScan(checkpoint *pb.ScanResponse) Cursor {
	if checkpoint.GetCursor() != "" {
		return Cursor{Vector: checkpoint.GetCursor()}
	}
	return Cursor{Revision: checkpoint.GetSnapshotRevision()}
}

func CursorFromChange(event *pb.WatchResponse) Cursor {
	if event.GetCursor() != "" {
		return Cursor{Vector: event.GetCursor()}
	}
	return Cursor{Revision: event.GetRevision()}
}

// OnChange delivers data events until cancellation, a handler error or a
// stream failure. It returns the last successfully processed cursor, including
// checkpoints, so the caller can resume without skipping a failed handler.
func (c *Client) OnChange(ctx context.Context, namespace, key string, after Cursor, handler func(*pb.WatchResponse) error) (Cursor, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.Watch(ctx, namespace, key, &after)
	if err != nil {
		return after, err
	}
	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return after, err
		}
		if !event.Checkpoint {
			if err := handler(event); err != nil {
				return after, err
			}
		}
		after = CursorFromChange(event)
	}
}

// NeedsRescan reports an expired cursor or a change in shard ownership.
func NeedsRescan(err error) bool {
	code := status.Code(err)
	return code == codes.OutOfRange || code == codes.FailedPrecondition
}
