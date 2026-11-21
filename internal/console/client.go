package console

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	api "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type client struct {
	conn    *grpc.ClientConn
	data    api.KVStoreClient
	console api.ConsoleClient
}

func Open(ctx context.Context, endpoint Endpoint, secret string) (Client, AccessKey, error) {
	address := strings.TrimSpace(endpoint.Address)
	if host, port, err := net.SplitHostPort(address); err != nil || host == "" || port == "" {
		return nil, AccessKey{}, errors.New("enter a server address in host:port format")
	}
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return nil, AccessKey{}, errors.New("enter an access key")
	}
	options, err := endpoint.Transport.DialOptions(secret)
	if err != nil {
		return nil, AccessKey{}, err
	}
	conn, err := grpc.NewClient(address, append(options, grpc.WithDisableRetry())...)
	if err != nil {
		return nil, AccessKey{}, err
	}
	c := &client{conn: conn, data: api.NewKVStoreClient(conn), console: api.NewConsoleClient(conn)}
	session, err := c.console.GetSession(ctx, &api.GetSessionRequest{})
	if err != nil {
		_ = c.Close()
		if status.Code(err) == codes.Unimplemented {
			return nil, AccessKey{}, errors.New("this server does not support console login; upgrade KVStore")
		}
		return nil, AccessKey{}, status.Error(status.Code(err), strings.ReplaceAll(status.Convert(err).Message(), secret, "[redacted]"))
	}
	if session.GetKey().GetName() == "" {
		_ = c.Close()
		return nil, AccessKey{}, errors.New("the server returned an invalid access-key session")
	}
	return c, fromKey(session.GetKey()), nil
}

func (c *client) Close() error { return c.conn.Close() }

func (c *client) Namespaces(ctx context.Context) ([]string, error) {
	response, err := c.console.ListNamespaces(ctx, &api.ListNamespacesRequest{})
	return response.GetNamespaces(), err
}

func (c *client) Scan(ctx context.Context, ns string, limit int) (ScanResult, error) {
	if limit < 1 {
		return ScanResult{}, errors.New("scan limit must be positive")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.data.Scan(ctx, &api.ScanRequest{Namespace: ns, UseVectorCursor: true})
	if err != nil {
		return ScanResult{}, err
	}
	var result ScanResult
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return ScanResult{}, err
		}
		if item.GetCheckpoint() {
			continue
		}
		if len(result.Records) == limit {
			result.More = true
			return result, nil
		}
		result.Records = append(result.Records, Record{Namespace: ns, Key: item.GetKey(), Revision: item.GetRevision(), ExpiresAt: asTime(item.GetExpiresAtUnixMs())})
	}
}

func (c *client) Get(ctx context.Context, ns, key string) (Record, bool, error) {
	response, err := c.data.Get(ctx, &api.GetRequest{Namespace: ns, Key: key, Consistency: api.ReadConsistency_READ_CONSISTENCY_STRONG})
	if err != nil || !response.GetFound() {
		return Record{}, false, err
	}
	return Record{Namespace: ns, Key: key, Value: response.GetValue(), Revision: response.GetRevision(), ExpiresAt: asTime(response.GetExpiresAtUnixMs())}, true, nil
}

func (c *client) Set(ctx context.Context, record Record) (Record, error) {
	response, err := c.data.Set(ctx, &api.SetRequest{Namespace: record.Namespace, Key: record.Key, Value: record.Value, ExpiresAtUnixMs: asMilliseconds(record.ExpiresAt)})
	if err != nil {
		return Record{}, err
	}
	record.Revision = response.GetRevision()
	record.ExpiresAt = asTime(response.GetExpiresAtUnixMs())
	return record, nil
}

func (c *client) Delete(ctx context.Context, ns, key string) (bool, error) {
	response, err := c.data.Delete(ctx, &api.DeleteRequest{Namespace: ns, Key: key})
	return response.GetDeleted(), err
}

func (c *client) Batch(ctx context.Context, operations []Mutation) ([]MutationResult, error) {
	request := &api.BatchWriteRequest{Operations: make([]*api.BatchWriteOperation, 0, len(operations))}
	for _, row := range operations {
		request.Operations = append(request.Operations, &api.BatchWriteOperation{
			Namespace: row.Namespace, Key: row.Key, Value: row.Value, Delete: row.Delete, ExpiresAtUnixMs: asMilliseconds(row.ExpiresAt),
		})
	}
	response, err := c.data.BatchWrite(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(response.GetResults()) != len(operations) {
		return nil, errors.New("the server returned an incomplete batch result")
	}
	result := make([]MutationResult, 0, len(operations))
	for _, item := range response.GetResults() {
		if item == nil {
			return nil, errors.New("the server returned an incomplete batch result")
		}
		row := MutationResult{Revision: item.GetRevision(), Deleted: item.GetDeleted()}
		if code := codes.Code(item.GetGrpcCode()); code != codes.OK {
			row.Error = code.String()
			if msg := item.GetError(); msg != "" {
				row.Error += ": " + msg
			}
		}
		result = append(result, row)
	}
	return result, nil
}

func (c *client) Head(ctx context.Context) (Head, error) {
	response, err := c.data.Head(ctx, &api.HeadRequest{UseVectorCursor: true})
	if err != nil {
		return Head{}, err
	}
	return Head{Revision: response.GetRevision(), OldestRevision: response.GetOldestAvailableRevision(), Cursor: response.GetCursor(), SupportsExpiration: response.GetSupportsExpiration()}, nil
}

func (c *client) Watch(ctx context.Context, ns, key string) (WatchStream, error) {
	setup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	head, err := c.Head(setup)
	if err != nil {
		return nil, err
	}
	stream, err := c.data.Watch(ctx, &api.WatchRequest{Namespace: ns, Key: key, AfterRevision: head.Revision, Cursor: head.Cursor, UseVectorCursor: true})
	if err != nil {
		return nil, err
	}
	return watchStream{stream}, nil
}

type watchStream struct {
	stream grpc.ServerStreamingClient[api.WatchResponse]
}

func (w watchStream) Next() (Event, error) {
	item, err := w.stream.Recv()
	if err != nil {
		return Event{}, err
	}
	return Event{Record: Record{Namespace: item.GetNamespace(), Key: item.GetKey(), Value: item.GetValue(), Revision: item.GetRevision(), ExpiresAt: asTime(item.GetExpiresAtUnixMs())}, Deleted: item.GetDeleted(), Checkpoint: item.GetCheckpoint()}, nil
}

func (c *client) ListKeys(ctx context.Context) ([]AccessKey, error) {
	response, err := c.console.ListAccessKeys(ctx, &api.ListAccessKeysRequest{})
	if err != nil {
		return nil, err
	}
	keys := make([]AccessKey, 0, len(response.GetKeys()))
	for _, key := range response.GetKeys() {
		keys = append(keys, fromKey(key))
	}
	return keys, nil
}

func (c *client) CreateKey(ctx context.Context, key AccessKey) (string, error) {
	response, err := c.console.CreateAccessKey(ctx, &api.CreateAccessKeyRequest{Key: &api.AccessKey{
		Name: key.Name, Read: key.Read, Write: key.Write, Manage: key.Manage, Namespaces: key.Namespaces,
	}})
	return response.GetSecret(), err
}

func (c *client) RevokeKey(ctx context.Context, name string) error {
	_, err := c.console.RevokeAccessKey(ctx, &api.RevokeAccessKeyRequest{Name: name})
	return err
}

func fromKey(key *api.AccessKey) AccessKey {
	return AccessKey{Name: key.GetName(), Read: key.GetRead(), Write: key.GetWrite(), Manage: key.GetManage(), Namespaces: key.GetNamespaces()}
}

func asTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func asMilliseconds(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}
