package kvstore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KeyValue is a namespace record encoded as a string.
type KeyValue struct {
	Key   string
	Value string
}

func (c *Client) GetKeyValue(ctx context.Context, namespace, key string) (string, bool, error) {
	record, err := c.getWithRetry(ctx, namespace, key)
	return string(record.GetValue()), record.GetFound(), err
}

// TextReader is the retrying key read used by ReadJSON.
type TextReader interface {
	GetKeyValue(context.Context, string, string) (string, bool, error)
}

// JSONReadOptions controls how ReadJSON handles an existing zero-length value.
type JSONReadOptions struct {
	EmptyAsMissing bool
}

// ReadJSON decodes a strong read using the same retry behavior as GetKeyValue.
// If found is true and err is non-nil, the stored value could not be decoded.
func ReadJSON[T any](ctx context.Context, c TextReader, namespace, key string, opts JSONReadOptions) (value T, found bool, err error) {
	raw, found, err := c.GetKeyValue(ctx, namespace, key)
	if err != nil || !found || (opts.EmptyAsMissing && raw == "") {
		return value, false, err
	}
	err = json.Unmarshal([]byte(raw), &value)
	return value, true, err
}

func (c *Client) getWithRetry(ctx context.Context, namespace, key string) (*pb.GetResponse, error) {
	ctx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	for {
		record, err := c.Get(ctx, namespace, key)
		if status.Code(err) != codes.Unavailable {
			return record, err
		}
		if err := waitForRetry(ctx); err != nil {
			return nil, err
		}
	}
}

func (c *Client) LoadKVStore(ctx context.Context, namespace string) ([]KeyValue, error) {
	ctx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	for {
		call, cancel := context.WithCancel(ctx)
		records, err := c.readNamespace(call, namespace)
		cancel()
		if status.Code(err) != codes.Unavailable {
			return records, err
		}
		if err := waitForRetry(ctx); err != nil {
			return nil, err
		}
	}
}

func (c *Client) readNamespace(ctx context.Context, namespace string) ([]KeyValue, error) {
	stream, err := c.Scan(ctx, namespace)
	if err != nil {
		return nil, err
	}
	result := make([]KeyValue, 0)
	for {
		record, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		if !record.GetCheckpoint() {
			result = append(result, KeyValue{Key: record.GetKey(), Value: string(record.GetValue())})
		}
	}
}

func (c *Client) GetAllKeyValues(ctx context.Context, namespace string) ([]KeyValue, error) {
	return c.LoadKVStore(ctx, namespace)
}

func (c *Client) SetKeyValue(ctx context.Context, namespace, key, value string, ttl time.Duration) error {
	return c.WithKeyLock(ctx, namespace, key, DefaultLockTTL, func(ctx context.Context) error {
		return c.Transaction(ctx, func(tx *Transaction) error {
			return tx.Set(namespace, key, value, ttl)
		})
	})
}

func (c *Client) DeleteKeyValue(ctx context.Context, namespace, key string) error {
	return c.WithKeyLock(ctx, namespace, key, DefaultLockTTL, func(ctx context.Context) error {
		return c.Transaction(ctx, func(tx *Transaction) error {
			return tx.Delete(namespace, key)
		})
	})
}
