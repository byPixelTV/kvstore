package kvstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CachedRecord is a local copy of a materialized namespace record.
type CachedRecord struct {
	Value           []byte
	Revision        uint64
	ExpiresAtUnixMS int64
}

func (r CachedRecord) expired(nowMS int64) bool {
	return r.ExpiresAtUnixMS > 0 && nowMS >= r.ExpiresAtUnixMS
}

func (r CachedRecord) clone() CachedRecord {
	r.Value = bytes.Clone(r.Value)
	return r
}

// NamespaceCache is a local materialized view maintained by Scan and Watch.
// Cached reads are synchronous and can lag server writes.
type NamespaceCache struct {
	cancel     context.CancelFunc
	done       chan struct{}
	wakeExpiry chan struct{}

	mu           sync.RWMutex
	entries      map[string]CachedRecord
	changed      chan struct{}
	version      uint64
	nextExpiryMS int64
	err          error
	stale        bool
}

// CacheNamespace loads a complete snapshot and starts a resumable watch.
// Cancel ctx, call Close on the cache, or close the Client to stop updates.
func (c *Client) CacheNamespace(ctx context.Context, namespace string) (*NamespaceCache, error) {
	if namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace cannot be empty")
	}
	run, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.cacheContext, cancel)
	if c.cacheContext.Err() != nil {
		cancel()
	}
	entries, cursor, err := c.scanCache(run, namespace)
	if err == nil {
		err = run.Err()
	}
	if err != nil {
		stop()
		cancel()
		return nil, err
	}
	cache := &NamespaceCache{
		cancel: cancel, done: make(chan struct{}),
		changed: make(chan struct{}), wakeExpiry: make(chan struct{}, 1),
	}
	cache.replace(entries)
	go func() {
		defer stop()
		expirationDone := make(chan struct{})
		go func() {
			defer close(expirationDone)
			cache.expire(run)
		}()
		err := c.watchCache(run, namespace, cursor, cache)
		cancel()
		<-expirationDone
		cache.mu.Lock()
		cache.err = err
		cache.stale = true
		close(cache.done)
		cache.mu.Unlock()
	}()
	return cache, nil
}

// Get returns a copy of a live record, or false if absent or expired.
func (c *NamespaceCache) Get(key string) (CachedRecord, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, found := c.entries[key]
	if !found || r.expired(time.Now().UnixMilli()) {
		return CachedRecord{}, false
	}
	return r.clone(), true
}

// Snapshot returns copies of live records together with their local version.
func (c *NamespaceCache) Snapshot() (map[string]CachedRecord, uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now().UnixMilli()
	result := make(map[string]CachedRecord, len(c.entries))
	for key, record := range c.entries {
		if !record.expired(now) {
			result[key] = record.clone()
		}
	}
	return result, c.version
}

// Version is a cache-local change generation, independent of server revisions.
// It also advances when an invalid cursor marks the cached snapshot stale.
func (c *NamespaceCache) Version() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

// WaitForChange waits for a version greater than after or for cancellation.
func (c *NamespaceCache) WaitForChange(ctx context.Context, after uint64) (uint64, error) {
	for {
		c.mu.RLock()
		version, changed, err := c.version, c.changed, c.err
		c.mu.RUnlock()
		if version > after || err != nil {
			return version, err
		}
		select {
		case <-changed:
		case <-c.done:
			err := c.Err()
			if err == nil {
				err = io.EOF
			}
			return c.Version(), err
		case <-ctx.Done():
			return version, ctx.Err()
		}
	}
}

// Stale reports that the cache is replacing an invalidated snapshot or has
// stopped updating. Reads continue to return its last materialized records.
func (c *NamespaceCache) Stale() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stale
}

// Done closes when the cache stops following updates.
func (c *NamespaceCache) Done() <-chan struct{} { return c.done }

// Err returns the reason updates stopped, or nil while they are still running.
func (c *NamespaceCache) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

func (c *NamespaceCache) Close() error {
	c.cancel()
	<-c.done
	return nil
}

func (c *NamespaceCache) signalChange() {
	c.version++
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *NamespaceCache) scheduleExpiry(deadline int64) {
	c.nextExpiryMS = deadline
	select {
	case c.wakeExpiry <- struct{}{}:
	default:
	}
}

func (c *NamespaceCache) replace(entries map[string]CachedRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = entries
	c.stale = false
	next, _ := c.pruneExpired(time.Now().UnixMilli())
	c.scheduleExpiry(next)
	c.signalChange()
}

func (c *NamespaceCache) markStale() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stale {
		c.stale = true
		c.signalChange()
	}
}

func (c *NamespaceCache) apply(event *pb.WatchResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := CachedRecord{
		Value: bytes.Clone(event.GetValue()), Revision: event.GetRevision(),
		ExpiresAtUnixMS: event.GetExpiresAtUnixMs(),
	}
	key := event.GetKey()
	if event.GetDeleted() || record.expired(time.Now().UnixMilli()) {
		if _, found := c.entries[key]; found {
			delete(c.entries, key)
			c.signalChange()
		}
		return nil
	}
	c.entries[key] = record
	if record.ExpiresAtUnixMS > 0 && (c.nextExpiryMS == 0 || record.ExpiresAtUnixMS < c.nextExpiryMS) {
		c.scheduleExpiry(record.ExpiresAtUnixMS)
	}
	c.signalChange()
	return nil
}

func (c *NamespaceCache) pruneExpired(nowMS int64) (next int64, changed bool) {
	for key, record := range c.entries {
		if record.expired(nowMS) {
			delete(c.entries, key)
			changed = true
		} else if record.ExpiresAtUnixMS > 0 && (next == 0 || record.ExpiresAtUnixMS < next) {
			next = record.ExpiresAtUnixMS
		}
	}
	return next, changed
}

func (c *NamespaceCache) expire(ctx context.Context) {
	for ctx.Err() == nil {
		c.mu.RLock()
		deadline := c.nextExpiryMS
		c.mu.RUnlock()
		var ticks <-chan time.Time
		var timer *time.Timer
		if deadline > 0 {
			timer = time.NewTimer(time.Until(time.UnixMilli(deadline)))
			ticks = timer.C
		}
		select {
		case <-ctx.Done():
		case <-c.wakeExpiry:
		case <-ticks:
			c.mu.Lock()
			next, changed := c.pruneExpired(time.Now().UnixMilli())
			c.nextExpiryMS = next
			if changed {
				c.signalChange()
			}
			c.mu.Unlock()
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (c *Client) scanCache(ctx context.Context, namespace string) (map[string]CachedRecord, Cursor, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		call, stop := context.WithCancel(ctx)
		entries, cursor, err := c.scanCacheOnce(call, namespace)
		stop()
		if !retryCacheStream(err) {
			return entries, cursor, err
		}
		if err := waitForRetry(ctx); err != nil {
			return nil, Cursor{}, err
		}
	}
}

func (c *Client) scanCacheOnce(ctx context.Context, namespace string) (map[string]CachedRecord, Cursor, error) {
	scan, err := c.Scan(ctx, namespace)
	if err != nil {
		return nil, Cursor{}, err
	}
	entries := make(map[string]CachedRecord)
	var checkpoint *pb.ScanResponse
	for {
		record, err := scan.Recv()
		if errors.Is(err, io.EOF) {
			if checkpoint == nil {
				return nil, Cursor{}, status.Error(codes.DataLoss, "namespace scan ended without checkpoint")
			}
			return entries, CursorFromScan(checkpoint), nil
		}
		if err != nil {
			return nil, Cursor{}, err
		}
		if record.GetCheckpoint() {
			if checkpoint != nil {
				return nil, Cursor{}, status.Error(codes.DataLoss, "duplicate namespace scan checkpoint")
			}
			checkpoint = record
			continue
		}
		entries[record.GetKey()] = CachedRecord{
			Value: bytes.Clone(record.GetValue()), Revision: record.GetRevision(),
			ExpiresAtUnixMS: record.GetExpiresAtUnixMs(),
		}
	}
}

func retryCacheStream(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Aborted, codes.DeadlineExceeded, codes.Canceled:
		return true
	default:
		return false
	}
}

func (c *Client) watchCache(ctx context.Context, namespace string, cursor Cursor, cache *NamespaceCache) error {
	rescan := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rescan {
			entries, next, err := c.scanCache(ctx, namespace)
			if err == nil {
				cache.replace(entries)
				cursor = next
				rescan = false
			} else if ctx.Err() != nil {
				return ctx.Err()
			} else if !retryCacheStream(err) && !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		}
		if !rescan {
			next, err := c.OnChange(ctx, namespace, "", cursor, cache.apply)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			cursor = next
			rescan = NeedsRescan(err)
			if rescan {
				cache.markStale()
			}
			if !rescan && !retryCacheStream(err) {
				return err
			}
		}
		retry := time.NewTimer(250 * time.Millisecond)
		select {
		case <-retry.C:
		case <-ctx.Done():
			retry.Stop()
			return ctx.Err()
		}
	}
}
