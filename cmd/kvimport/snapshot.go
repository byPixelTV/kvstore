package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"github.com/byPixelTV/kvstore/internal/clienttls"
)

const legacySnapshotHeader = "KVSTORE-IMPORT-SNAPSHOT 1"
const snapshotHeader = "KVSTORE-IMPORT-SNAPSHOT 2"
const snapshotFooter = "manifest "
const snapshotMaxLine = 2 << 20

type snapshotManifest struct {
	Records         int64    `json:"records"`
	Expiring        int64    `json:"expiring,omitempty"`
	Expired         int64    `json:"expired"`
	SkippedExpiring int64    `json:"skipped_expiring"`
	Namespaces      []string `json:"namespaces"`
	SHA256          string   `json:"sha256"`
}

type expectedRecord struct {
	digest          [sha256.Size]byte
	expiresAtUnixMS int64
}

type snapshotIndex map[string]map[string]expectedRecord

func expectRecord(value []byte, expiresAtUnixMS int64) expectedRecord {
	return expectedRecord{digest: sha256.Sum256(value), expiresAtUnixMS: expiresAtUnixMS}
}

func exportSnapshot(ctx context.Context, path string, collections []selection, produce func(*totals, func(*kv.SetRequest) error) error) (err error) {
	if filepath.Ext(path) != ".kvsnapshot" {
		return errors.New("snapshot output must have the .kvsnapshot extension")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = file.Close()
			err = errors.Join(err, os.Remove(path))
		}
	}()
	namespaces := make(map[string]struct{})
	for _, collection := range collections {
		switch {
		case collection.name == "single_value":
			namespaces[singleNS] = struct{}{}
		case strings.HasPrefix(collection.name, storePrefix):
			namespaces[strings.TrimPrefix(collection.name, storePrefix)] = struct{}{}
		}
	}
	manifest, err := writeSnapshot(ctx, file, namespaces, produce)
	if err != nil {
		return err
	}
	if err = errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	fmt.Printf("snapshot exported: records=%d expiring=%d expired=%d skipped_expiring=%d namespaces=%d sha256=%s\n",
		manifest.Records, manifest.Expiring, manifest.Expired, manifest.SkippedExpiring, len(manifest.Namespaces), manifest.SHA256)
	return nil
}

func writeSnapshot(ctx context.Context, output io.Writer, namespaces map[string]struct{}, produce func(*totals, func(*kv.SetRequest) error) error) (snapshotManifest, error) {
	var manifest snapshotManifest
	out := bufio.NewWriter(output)
	if _, err := fmt.Fprintln(out, snapshotHeader); err != nil {
		return manifest, err
	}
	hash := sha256.New()
	rows := json.NewEncoder(io.MultiWriter(out, hash))
	var stats totals
	err := produce(&stats, func(row *kv.SetRequest) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		namespaces[row.GetNamespace()] = struct{}{}
		if row.GetExpiresAtUnixMs() > 0 {
			manifest.Expiring++
		}
		return rows.Encode(row)
	})
	if err != nil {
		return manifest, err
	}
	manifest.Records, manifest.Expired, manifest.SkippedExpiring = stats.records, stats.expired, stats.skippedExpiring
	for namespace := range namespaces {
		manifest.Namespaces = append(manifest.Namespaces, namespace)
	}
	sort.Strings(manifest.Namespaces)
	// Include the manifest's metadata in the digest, with the digest field empty.
	manifest.SHA256, err = snapshotDigest(hash, manifest)
	if err != nil {
		return manifest, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return manifest, err
	}
	if len(snapshotFooter)+len(encoded)+1 >= snapshotMaxLine {
		return manifest, errors.New("snapshot manifest exceeds the format size limit")
	}
	if _, err := fmt.Fprintf(out, "%s%s\n", snapshotFooter, encoded); err != nil {
		return manifest, err
	}
	return manifest, out.Flush()
}

func snapshotDigest(hash hash.Hash, manifest snapshotManifest) (string, error) {
	manifest.SHA256 = ""
	if err := json.NewEncoder(hash).Encode(manifest); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func walkSnapshot(ctx context.Context, input io.Reader, emit func(*kv.SetRequest) error) (snapshotManifest, error) {
	scanner := bufio.NewScanner(input)
	// A 256 KiB record may expand via JSON escaping or base64.
	scanner.Buffer(make([]byte, 64<<10), snapshotMaxLine)
	var manifest snapshotManifest
	if !scanner.Scan() || (scanner.Text() != snapshotHeader && scanner.Text() != legacySnapshotHeader) {
		return manifest, errors.New("invalid snapshot header")
	}
	legacy := scanner.Text() == legacySnapshotHeader
	hash := sha256.New()
	var count, expiring int64
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return manifest, err
		}
		line := scanner.Bytes()
		if tail, found := bytes.CutPrefix(line, []byte(snapshotFooter)); found {
			if err := json.Unmarshal(tail, &manifest); err != nil {
				return manifest, errors.New("invalid snapshot manifest")
			}
			digest, err := snapshotDigest(hash, manifest)
			if err != nil || digest != manifest.SHA256 || manifest.Records != count || manifest.Expiring != expiring ||
				manifest.Expired < 0 || manifest.SkippedExpiring < 0 {
				return manifest, errors.New("snapshot checksum or record count mismatch")
			}
			if scanner.Scan() {
				return manifest, errors.New("unexpected data after snapshot manifest")
			}
			return manifest, scanner.Err()
		}
		_, _ = hash.Write(line)
		_, _ = hash.Write([]byte{'\n'})
		var row kv.SetRequest
		if err := json.Unmarshal(line, &row); err != nil {
			return manifest, fmt.Errorf("invalid snapshot record %d", count+1)
		}
		if err := validateRecord(&row); err != nil {
			return manifest, fmt.Errorf("snapshot record %d: %w", count+1, err)
		}
		if row.GetExpiresAtUnixMs() > 0 {
			if legacy {
				return manifest, errors.New("snapshot v1 cannot contain expiration deadlines")
			}
			expiring++
		}
		count++
		if err := emit(&row); err != nil {
			return manifest, fmt.Errorf("snapshot record %d: %w", count, err)
		}
	}
	return manifest, errors.Join(errors.New("snapshot ended before its manifest"), scanner.Err())
}

func loadSnapshot(ctx context.Context, input io.Reader) (snapshotManifest, snapshotIndex, error) {
	index := make(snapshotIndex)
	manifest, err := walkSnapshot(ctx, input, func(row *kv.SetRequest) error {
		if index[row.Namespace] == nil {
			index[row.Namespace] = make(map[string]expectedRecord)
		}
		if _, exists := index[row.Namespace][row.Key]; exists {
			return errors.New("duplicate namespace and key")
		}
		index[row.Namespace][row.Key] = expectRecord(row.Value, row.GetExpiresAtUnixMs())
		return nil
	})
	if err != nil {
		return manifest, nil, err
	}
	known := make(map[string]bool, len(manifest.Namespaces))
	for _, namespace := range manifest.Namespaces {
		if known[namespace] || validateRecord(&kv.SetRequest{Namespace: namespace, Key: "."}) != nil {
			return manifest, nil, errors.New("invalid or duplicate snapshot namespace")
		}
		known[namespace] = true
	}
	for namespace := range index {
		if !known[namespace] {
			return manifest, nil, errors.New("record namespace missing from manifest")
		}
	}
	return manifest, index, nil
}

func runSnapshot(ctx context.Context, path, addr string, apply, verify, showConflicts bool, transport clienttls.Options) error {
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()
	manifest, index, err := loadSnapshot(ctx, input)
	if err != nil {
		return err
	}
	fmt.Printf("snapshot validated: records=%d expiring=%d expired=%d skipped_expiring=%d namespaces=%d sha256=%s\n",
		manifest.Records, manifest.Expiring, manifest.Expired, manifest.SkippedExpiring, len(manifest.Namespaces), manifest.SHA256)
	if addr == "" {
		return nil
	}
	conn, err := connectTarget(addr, transport)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := kv.NewKVStoreClient(conn)
	if manifest.Expiring > 0 {
		if err := requireTargetExpiration(ctx, client); err != nil {
			return err
		}
	}
	if verify {
		stats, err := verifySnapshot(ctx, client, manifest.Namespaces, index, showConflicts)
		if err != nil {
			return err
		}
		fmt.Printf("verification: namespaces=%d matching=%d missing=%d changed=%d extra=%d expired_since_export=%d\n",
			len(manifest.Namespaces), stats.matching, stats.missing, stats.changed, stats.extra, stats.expired)
		if stats.missing+stats.changed+stats.extra != 0 {
			return errors.New("target differs from snapshot")
		}
		return nil
	}
	fmt.Printf("checking destination: namespaces=%d\n", len(manifest.Namespaces))
	stats, matching, err := planSnapshotImport(ctx, client, manifest, index, showConflicts)
	if err != nil {
		return err
	}
	if !apply || stats.conflicts > 0 {
		reportDestination(false, stats, nil)
		if stats.conflicts > 0 {
			return fmt.Errorf("destination contains %d conflicting keys", stats.conflicts)
		}
		return nil
	}
	fmt.Printf("importing: pending=%d already_matching=%d\n", stats.newKeys, stats.unchanged)
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	err = applySnapshot(ctx, client, input, manifest, index, matching, &stats)
	reportDestination(true, stats, err)
	return err
}

type verificationTotals struct {
	matching, missing, changed, extra, expired int64
}

func verifySnapshot(ctx context.Context, client kv.KVStoreClient, namespaces []string, index snapshotIndex, showConflicts bool) (verificationTotals, error) {
	var stats verificationTotals
	for _, namespace := range namespaces {
		call, cancel := context.WithTimeout(ctx, 5*time.Minute)
		err := verifyNamespace(call, client, namespace, index[namespace], showConflicts, &stats)
		cancel()
		if err != nil {
			return stats, fmt.Errorf("verify namespace %q: %w", namespace, err)
		}
	}
	return stats, nil
}

func verifyNamespace(ctx context.Context, client kv.KVStoreClient, namespace string, expected map[string]expectedRecord, showConflicts bool, stats *verificationTotals) error {
	err := scanTargetNamespace(ctx, client, namespace, len(expected), func(item *kv.ScanResponse) {
		key := item.GetKey()
		want, found := expected[key]
		switch {
		case !found:
			stats.extra++
			printDifference(showConflicts, "extra", namespace, key)
		case want != expectRecord(item.GetValue(), item.GetExpiresAtUnixMs()):
			stats.changed++
			printDifference(showConflicts, "changed", namespace, key)
		default:
			stats.matching++
		}
		delete(expected, key)
	})
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	for key, record := range expected {
		if deadlinePassed(record.expiresAtUnixMS, now) {
			stats.expired++
			continue
		}
		stats.missing++
		printDifference(showConflicts, "missing", namespace, key)
	}
	return nil
}

func scanTargetNamespace(ctx context.Context, client kv.KVStoreClient, namespace string, sizeHint int, receive func(*kv.ScanResponse)) error {
	stream, err := client.Scan(ctx, &kv.ScanRequest{Namespace: namespace, UseVectorCursor: true})
	if err != nil {
		return err
	}
	seen := make(map[string]bool, sizeHint)
	var initial, final bool
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if final {
			return errors.New("scan returned data after final checkpoint")
		}
		if item.GetCheckpoint() {
			switch {
			case item.GetCursor() != "" && !initial:
				final = true
			case item.GetCursor() == "" && !initial && len(seen) == 0:
				initial = true
			default:
				return errors.New("unexpected scan checkpoint")
			}
			continue
		}
		key := item.GetKey()
		if key == "" || seen[key] {
			return errors.New("scan returned an empty or duplicate key")
		}
		seen[key] = true
		receive(item)
	}
	if !initial && !final {
		return errors.New("scan ended without a checkpoint")
	}
	return nil
}

func printDifference(enabled bool, kind, namespace, key string) {
	if enabled {
		fmt.Printf("%s: namespace=%q key=%q\n", kind, namespace, key)
	}
}
