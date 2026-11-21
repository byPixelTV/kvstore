package kvstore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrTransactionConflict = errors.New("kvstore: transaction precondition failed")

// Transact can safely retry the same request ID with the same checks and writes
// if the outcome is unknown. A false result means no writes were committed.
func (c *Client) Transact(ctx context.Context, request *pb.TransactionRequest) (bool, error) {
	reply, err := c.rpc.Transact(c.authorized(ctx), request)
	return reply.GetCommitted(), err
}

func (c *Client) transact(ctx context.Context, checks []*pb.TransactionCheck, operations []*pb.BatchWriteOperation) (bool, error) {
	ctx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	request := &pb.TransactionRequest{RequestId: rand.Text(), Checks: checks, Operations: operations}
	for {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		committed, err := c.Transact(call, request)
		cancel()
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
			if waitErr := waitForRetry(ctx); waitErr != nil {
				return false, fmt.Errorf("kvstore: transaction %s outcome unknown: %w", request.RequestId, waitErr)
			}
		default:
			return committed, err
		}
	}
}

func waitForRetry(ctx context.Context) error {
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type KeyRef struct {
	StoreName string
	Key       string
}

type transactionWrite struct {
	value  string
	ttl    time.Duration
	delete bool
}

// Transaction stages an atomic conditional commit. The callback runs once.
// Strong reads and lease ownership are verified again before any write.
type Transaction struct {
	client *Client
	ctx    context.Context
	reads  map[KeyRef]*pb.GetResponse
	writes map[KeyRef]transactionWrite
}

func (c *Client) Transaction(ctx context.Context, fn func(*Transaction) error) error {
	if fn == nil {
		return errors.New("kvstore: transaction callback is nil")
	}
	tx := &Transaction{client: c, ctx: ctx, reads: make(map[KeyRef]*pb.GetResponse), writes: make(map[KeyRef]transactionWrite)}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.commit()
}

func (t *Transaction) Get(namespace, key string) (string, bool, error) {
	ref := KeyRef{namespace, key}
	if write, ok := t.writes[ref]; ok {
		return write.value, !write.delete, nil
	}
	record := t.reads[ref]
	if record == nil {
		var err error
		record, err = t.client.getWithRetry(t.ctx, namespace, key)
		if err != nil {
			return "", false, err
		}
		t.reads[ref] = record
	}
	return string(record.GetValue()), record.GetFound(), nil
}

func (t *Transaction) Set(namespace, key, value string, ttl time.Duration) error {
	if ttl < 0 {
		return errors.New("kvstore: TTL must be nonnegative")
	}
	t.writes[KeyRef{namespace, key}] = transactionWrite{value: value, ttl: ttl}
	return nil
}

func (t *Transaction) Delete(namespace, key string) error {
	t.writes[KeyRef{namespace, key}] = transactionWrite{delete: true}
	return nil
}

func sortedKeys[V any](m map[KeyRef]V) []KeyRef {
	keys := make([]KeyRef, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b KeyRef) int {
		if cmp := strings.Compare(a.StoreName, b.StoreName); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Key, b.Key)
	})
	return keys
}

func revisionCheck(namespace, key string, revision uint64) *pb.TransactionCheck {
	return &pb.TransactionCheck{Namespace: namespace, Key: key, Expected: &pb.TransactionCheck_Revision{Revision: revision}}
}

func (t *Transaction) commit() error {
	checks := heldLeaseChecks(t.ctx)
	for _, ref := range sortedKeys(t.reads) {
		checks = append(checks, revisionCheck(ref.StoreName, ref.Key, t.reads[ref].GetRevision()))
	}
	writes := make([]*pb.BatchWriteOperation, 0, len(t.writes))
	now := time.Now()
	for _, ref := range sortedKeys(t.writes) {
		write := t.writes[ref]
		deadline := int64(0)
		if write.ttl > 0 {
			deadline = now.Add(write.ttl).UnixMilli()
		}
		writes = append(writes, &pb.BatchWriteOperation{
			Namespace: ref.StoreName, Key: ref.Key, Value: []byte(write.value),
			Delete: write.delete, ExpiresAtUnixMs: deadline,
		})
	}
	if len(checks)+len(writes) == 0 {
		return t.ctx.Err()
	}
	committed, err := t.client.transact(t.ctx, checks, writes)
	if err != nil {
		return err
	}
	if !committed {
		return ErrTransactionConflict
	}
	return nil
}

// TransformKeyValue runs the update under the same distributed key lock as
// other compatible writers. The updated record is committed conditionally.
func (c *Client) TransformKeyValue(ctx context.Context, namespace, key string, ttl time.Duration, update func(string, bool) (string, error)) error {
	return c.WithKeyLock(ctx, namespace, key, DefaultLockTTL, func(ctx context.Context) error {
		return c.Transaction(ctx, func(tx *Transaction) error {
			current, exists, err := tx.Get(namespace, key)
			if err != nil {
				return err
			}
			next, err := update(current, exists)
			if err != nil {
				return err
			}
			return tx.Set(namespace, key, next, ttl)
		})
	})
}
