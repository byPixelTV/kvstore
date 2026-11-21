package kvstore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
)

const (
	LeaseNamespace    = "kvstore-leases"
	DefaultLockTTL    = 10 * time.Second
	leaseCommitWindow = 3 * time.Second
)

var ErrLeaseLost = errors.New("kvstore: distributed lock was lost")
var errReleaseLease = errors.New("kvstore: lease callback completed")

type heldLeaseKey struct{}
type heldLease struct {
	key, token string
	parent     *heldLease
}

func leaseKey(namespace, key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(namespace)) + ":" + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func (l *heldLease) check() *pb.TransactionCheck {
	return &pb.TransactionCheck{Namespace: LeaseNamespace, Key: l.key, Expected: &pb.TransactionCheck_Value{Value: []byte(l.token)}}
}

func heldLeaseChecks(ctx context.Context) []*pb.TransactionCheck {
	var checks []*pb.TransactionCheck
	for l, _ := ctx.Value(heldLeaseKey{}).(*heldLease); l != nil; l = l.parent {
		checks = append(checks, l.check())
	}
	return checks
}

func (c *Client) writeLease(ctx context.Context, lease *heldLease, check *pb.TransactionCheck, expiry time.Time) (bool, error) {
	write := &pb.BatchWriteOperation{Namespace: LeaseNamespace, Key: lease.key, Delete: expiry.IsZero()}
	if !write.Delete {
		write.Value = []byte(lease.token)
		write.ExpiresAtUnixMs = expiry.UnixMilli()
	}
	return c.transact(ctx, []*pb.TransactionCheck{check}, []*pb.BatchWriteOperation{write})
}

// WithKeyLock renews an expiring, owner-checked lease while fn runs. The
// callback's context is canceled if ownership is lost. Use that context for
// transactions so their commit also checks the lease owner.
func (c *Client) WithKeyLock(ctx context.Context, namespace, key string, ttl time.Duration, fn func(context.Context) error) (err error) {
	if namespace == "" || key == "" || ttl < 5*time.Second || fn == nil {
		return errors.New("kvstore: a lock requires a key, a callback and a TTL of at least five seconds")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	parent, _ := ctx.Value(heldLeaseKey{}).(*heldLease)
	encoded := leaseKey(namespace, key)
	for l := parent; l != nil; l = l.parent {
		if l.key == encoded {
			return fn(ctx)
		}
	}
	l := &heldLease{key: encoded, token: rand.Text(), parent: parent}
	acquireCtx, stopAcquire := context.WithTimeout(ctx, DefaultLockTTL)
	defer stopAcquire()
	var expires time.Time
	for {
		record, err := c.getWithRetry(acquireCtx, LeaseNamespace, l.key)
		if err != nil {
			return err
		}
		if record.GetFound() {
			if err := waitForRetry(acquireCtx); err != nil {
				return err
			}
			continue
		}
		expires = time.Now().Add(ttl)
		owned, err := c.writeLease(acquireCtx, l, revisionCheck(LeaseNamespace, l.key, 0), expires)
		if err != nil {
			return err
		}
		if owned {
			break
		}
		if err := waitForRetry(acquireCtx); err != nil {
			return err
		}
	}
	stopAcquire()
	initialExpiry := expires
	// Sharded transactions require an owned record to outlive preparation.
	// Use half the spare time if a short TTL has less room for a slow RPC.
	renewAfter := min(ttl/3, (ttl-leaseCommitWindow)/2)

	lockCtx, cancel := context.WithCancelCause(context.WithValue(ctx, heldLeaseKey{}, l))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			renewAt := time.NewTimer(time.Until(expires.Add(renewAfter - ttl)))
			select {
			case <-lockCtx.Done():
				renewAt.Stop()
				return
			case <-renewAt.C:
			}
			renewCtx, stop := context.WithDeadline(lockCtx, expires)
			next := time.Now().Add(ttl)
			owned, err := c.writeLease(renewCtx, l, l.check(), next)
			stop()
			if !owned || err != nil {
				cancel(ErrLeaseLost)
				return
			}
			expires = next
		}
	}()
	defer func() {
		cancel(errReleaseLease)
		<-done
		cause := context.Cause(lockCtx)
		if errors.Is(cause, errReleaseLease) {
			cause = nil
		}
		releaseCtx, doneRelease := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer doneRelease()
		released, releaseErr := c.writeLease(releaseCtx, l, l.check(), time.Time{})
		if !released && releaseErr == nil {
			releaseErr = ErrLeaseLost
		}
		err = errors.Join(err, cause, releaseErr)
	}()
	if !time.Now().Before(initialExpiry) {
		return ErrLeaseLost
	}
	return fn(lockCtx)
}
