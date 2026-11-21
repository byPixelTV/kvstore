// kvimport migrates authoritative KVStore v1 documents from a frozen MongoDB source.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	kv "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTarget     = "127.0.0.1:50081"
	storePrefix       = "kv_store_"
	singleNS          = "legacy-v1-single"
	counterPrefix     = "legacy-v1-counter/"
	journalCollection = "__kvstore_transaction_journal"
)

type document struct {
	Key           string     `bson:"key"`
	Store         string     `bson:"storeName"`
	Value         *string    `bson:"value"`
	Count         *int64     `bson:"count"`
	ExpiresAt     *int64     `bson:"expiresAt"`
	ExpiresAtDate *time.Time `bson:"expiresAtDate"`
}

type selection struct {
	name string
	keys bson.D
}

type totals struct {
	records, expired, skippedExpiring, written, unchanged int64
	newKeys, conflicts, expiring, expiredSinceValidation  int64
}

func main() {
	database := flag.String("database", "", "source MongoDB database")
	addr := flag.String("addr", "", "destination gRPC address (apply defaults to "+defaultTarget+")")
	dryRun := flag.Bool("dry-run", false, "validate the source and, when -addr is set, check the destination without writes (default mode)")
	apply := flag.Bool("apply", false, "write to KVStore after source validation")
	exportPath := flag.String("export-file", "", "capture MongoDB data to a new .kvsnapshot file")
	inputPath := flag.String("input-file", "", "use a .kvsnapshot file as the source")
	verify := flag.Bool("verify", false, "compare a snapshot with the target in both directions within its namespaces")
	showConflicts := flag.Bool("show-conflicts", false, "print the namespaces and keys that differ")
	skipExpiring := flag.Bool("skip-expiring", false, "omit live records with a TTL")
	ca := flag.String("tls-ca", "", "destination CA PEM (enables TLS)")
	cert := flag.String("tls-cert", "", "destination client certificate PEM")
	key := flag.String("tls-key", "", "destination client private key PEM")
	flag.Parse()
	if (*dryRun && (*apply || *verify || *exportPath != "")) || (*apply && (*verify || *exportPath != "")) {
		log.Fatal("-dry-run, -apply, -verify and -export-file are separate modes")
	}
	if *showConflicts && (*apply || *addr == "") {
		log.Fatal("-show-conflicts requires a destination dry run or verify with -addr")
	}
	if *exportPath != "" && (*inputPath != "" || *addr != "" || *verify) {
		log.Fatal("-export-file takes a MongoDB source only")
	}
	if *inputPath != "" && (*database != "" || *skipExpiring) {
		log.Fatal("-input-file already supplies the source and TTL selection")
	}
	if *verify && (*inputPath == "" || *addr == "") {
		log.Fatal("-verify requires -input-file and -addr")
	}
	if *apply && *addr == "" {
		*addr = defaultTarget
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if *inputPath != "" {
		err = runSnapshot(ctx, *inputPath, *addr, *apply, *verify, *showConflicts, *ca, *cert, *key)
	} else {
		err = run(ctx, *database, *addr, *exportPath, *apply, *skipExpiring, *showConflicts, *ca, *cert, *key)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, database, addr, exportPath string, apply, skipExpiring, showConflicts bool, ca, cert, key string) error {
	uri := os.Getenv("KVSTORE_MONGO_URI")
	if uri == "" || database == "" {
		return errors.New("set KVSTORE_MONGO_URI and pass -database")
	}
	opts := options.Client().ApplyURI(uri)
	if opts.Validate() != nil {
		return errors.New("invalid KVSTORE_MONGO_URI")
	}
	source, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("create MongoDB client: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = source.Disconnect(ctx)
	}()
	db := source.Database(database)
	collections, err := sourceCollections(ctx, db)
	if err != nil {
		return err
	}
	if len(collections) == 0 {
		return errors.New("no KVStore v1 collections found")
	}
	var validated totals
	asOf := time.Now()
	if exportPath != "" {
		return exportSnapshot(ctx, exportPath, collections, func(stats *totals, emit func(*kv.SetRequest) error) error {
			return walk(ctx, db, collections, asOf, skipExpiring, stats, emit)
		})
	}
	if err := walk(ctx, db, collections, asOf, skipExpiring, &validated, nil); err != nil {
		return err
	}
	fmt.Printf("source validation passed: records=%d expiring=%d expired=%d skipped_expiring=%d\n",
		validated.records, validated.expiring, validated.expired, validated.skippedExpiring)
	if addr == "" {
		fmt.Println("dry run complete: source checked")
		return nil
	}
	conn, err := connectTarget(addr, ca, cert, key)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := kv.NewKVStoreClient(conn)
	if validated.expiring > 0 {
		if err := requireTargetExpiration(ctx, client); err != nil {
			return err
		}
	}
	var destination totals
	err = walk(ctx, db, collections, asOf, skipExpiring, &destination, func(record *kv.SetRequest) error {
		return compareAndReport(ctx, client, record, apply, showConflicts, &destination)
	})
	reportDestination(apply, destination, err)
	if err != nil {
		return err
	}
	if validated.records != destination.records || validated.expired != destination.expired ||
		validated.skippedExpiring != destination.skippedExpiring || validated.expiring != destination.expiring {
		return fmt.Errorf("source counts changed during the run (records %d → %d, expired %d → %d, skipped_expiring %d → %d, expiring %d → %d)",
			validated.records, destination.records, validated.expired, destination.expired,
			validated.skippedExpiring, destination.skippedExpiring, validated.expiring, destination.expiring)
	}
	if destination.conflicts > 0 {
		return fmt.Errorf("destination contains %d conflicting keys", destination.conflicts)
	}
	return nil
}

func compareAndReport(ctx context.Context, client kv.KVStoreClient, record *kv.SetRequest, apply, showConflicts bool, stats *totals) error {
	before := stats.conflicts
	if err := compareRecord(ctx, client, record, apply, stats); err != nil {
		return err
	}
	if stats.conflicts != before {
		printDifference(showConflicts, "conflict", record.GetNamespace(), record.GetKey())
	}
	return nil
}

func reportDestination(apply bool, destination totals, err error) {
	if apply {
		fmt.Printf("destination: acknowledged=%d already_matching=%d expired=%d skipped_expiring=%d expired_since_validation=%d\n",
			destination.written, destination.unchanged, destination.expired, destination.skippedExpiring, destination.expiredSinceValidation)
	} else if err == nil {
		fmt.Printf("dry run complete: would_write=%d already_matching=%d conflicts=%d expired_since_validation=%d\n",
			destination.newKeys, destination.unchanged, destination.conflicts, destination.expiredSinceValidation)
	}
}

func compareRecord(ctx context.Context, client kv.KVStoreClient, record *kv.SetRequest, apply bool, stats *totals) error {
	if deadlinePassed(record.GetExpiresAtUnixMs(), time.Now().UnixMilli()) {
		stats.expiredSinceValidation++
		return nil
	}
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	existing, err := client.Get(call, &kv.GetRequest{Namespace: record.Namespace, Key: record.Key})
	if err != nil {
		return fmt.Errorf("check destination: %w", err)
	}
	if existing.GetFound() {
		if bytes.Equal(existing.GetValue(), record.GetValue()) && existing.GetExpiresAtUnixMs() == record.GetExpiresAtUnixMs() {
			stats.unchanged++
			return nil
		}
		if apply {
			return errors.New("destination already contains a different value or deadline")
		}
		stats.conflicts++
		return nil
	}
	if !apply {
		stats.newKeys++
		return nil
	}
	if err := writeRecord(call, client, record); err != nil {
		return err
	}
	stats.written++
	return nil
}

func writeRecord(ctx context.Context, client kv.KVStoreClient, record *kv.SetRequest) error {
	reply, err := client.Set(ctx, record)
	if err != nil {
		return fmt.Errorf("write outcome may be unknown: %w", err)
	}
	if reply.GetRevision() == 0 {
		return errors.New("destination returned no write revision")
	}
	if reply.GetExpiresAtUnixMs() != record.GetExpiresAtUnixMs() {
		return errors.New("destination acknowledged a different expiration")
	}
	return nil
}

func deadlinePassed(deadline, now int64) bool { return deadline > 0 && deadline <= now }

func requireTargetExpiration(ctx context.Context, client kv.KVStoreClient) error {
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	head, err := client.Head(call, &kv.HeadRequest{UseVectorCursor: true})
	if err != nil {
		return fmt.Errorf("check expiration support: %w", err)
	}
	if !head.GetSupportsExpiration() {
		return errors.New("target does not support expiration; upgrade all routers and replicas before importing TTL data")
	}
	return nil
}

func targetCredentials(ca, cert, key string) (credentials.TransportCredentials, error) {
	if ca == "" && cert == "" && key == "" {
		return insecure.NewCredentials(), nil
	}
	if ca == "" || (cert == "") != (key == "") {
		return nil, errors.New("TLS requires -tls-ca and both or neither of -tls-cert and -tls-key")
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("TLS CA file contains no valid certificates")
	}
	cfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return credentials.NewTLS(cfg), nil
}

func connectTarget(addr, ca, cert, key string) (*grpc.ClientConn, error) {
	creds, err := targetCredentials(ca, cert, key)
	if err != nil {
		return nil, err
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(creds), grpc.WithDisableRetry())
}

func sourceCollections(ctx context.Context, db *mongo.Database) ([]selection, error) {
	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var out []selection
	for _, name := range names {
		switch {
		case name == journalCollection:
			err := db.Collection(name).FindOne(ctx, bson.D{}).Err()
			if err == nil {
				return nil, errors.New("v1 transaction journal is nonempty; finish v1 recovery before exporting")
			}
			if !errors.Is(err, mongo.ErrNoDocuments) {
				return nil, err
			}
		case name == "single_value":
			out = append(out, selection{name, bson.D{{Key: "storeName", Value: 1}}})
		case name == "kv_counters":
			out = append(out, selection{name, bson.D{{Key: "storeName", Value: 1}, {Key: "key", Value: 1}}})
		case strings.HasPrefix(name, storePrefix):
			namespace := strings.TrimPrefix(name, storePrefix)
			if namespace == "" || namespace == singleNS || strings.HasPrefix(namespace, counterPrefix) ||
				strings.HasPrefix(namespace, "__kvstore_internal_") {
				return nil, fmt.Errorf("legacy store %q collides with a reserved namespace", namespace)
			}
			out = append(out, selection{name, bson.D{{Key: "key", Value: 1}}})
		}
	}
	return out, nil
}

func walk(ctx context.Context, db *mongo.Database, collections []selection, asOf time.Time, skipExpiring bool, stats *totals, write func(*kv.SetRequest) error) error {
	for _, collection := range collections {
		if err := walkCollection(ctx, db.Collection(collection.name), collection, asOf, skipExpiring, stats, write); err != nil {
			return fmt.Errorf("collection %q: %w", collection.name, err)
		}
	}
	return nil
}

func walkCollection(ctx context.Context, coll *mongo.Collection, selection selection, asOf time.Time, skipExpiring bool, stats *totals, write func(*kv.SetRequest) error) error {
	find := options.Find().SetSort(selection.keys).SetCollation(&options.Collation{Locale: "simple"}).SetAllowDiskUse(true)
	cursor, err := coll.Find(ctx, bson.D{}, find)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = cursor.Close(closeCtx)
	}()
	var lastNS, lastKey string
	for index := 1; cursor.Next(ctx); index++ {
		var doc document
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("document %d: %w", index, err)
		}
		record, err := mapDocument(selection.name, doc)
		if err != nil {
			return fmt.Errorf("document %d: %w", index, err)
		}
		if record.Namespace == lastNS && record.Key == lastKey {
			return fmt.Errorf("document %d duplicates a destination key", index)
		}
		lastNS, lastKey = record.Namespace, record.Key
		if deadline, hasDeadline := mongoDeadline(doc); hasDeadline {
			if deadline <= asOf.UnixMilli() {
				stats.expired++
				continue
			}
			if skipExpiring {
				stats.skippedExpiring++
				continue
			}
			record.ExpiresAtUnixMs = deadline
			stats.expiring++
		}
		stats.records++
		if write != nil {
			if err := write(record); err != nil {
				return fmt.Errorf("document %d: %w", index, err)
			}
		}
	}
	return cursor.Err()
}

func mongoDeadline(doc document) (int64, bool) {
	if doc.ExpiresAtDate != nil {
		date := doc.ExpiresAtDate.UnixMilli()
		if doc.ExpiresAt == nil || date < *doc.ExpiresAt {
			return date, true
		}
	}
	if doc.ExpiresAt != nil {
		return *doc.ExpiresAt, true
	}
	return 0, false
}

func mapDocument(collection string, doc document) (*kv.SetRequest, error) {
	record := &kv.SetRequest{Namespace: strings.TrimPrefix(collection, storePrefix), Key: doc.Key}
	switch collection {
	case "single_value":
		record.Namespace, record.Key = singleNS, doc.Store
	case "kv_counters":
		if doc.Store == "" || doc.Count == nil {
			return nil, errors.New("counter requires a store and count")
		}
		record.Namespace, record.Value = counterPrefix+doc.Store, strconv.AppendInt(nil, *doc.Count, 10)
	}
	if collection != "kv_counters" {
		if doc.Value == nil {
			return nil, errors.New("missing string value")
		}
		record.Value = []byte(*doc.Value)
	}
	return record, validateRecord(record)
}

func validateRecord(record *kv.SetRequest) error {
	if record.Namespace == "" || strings.HasPrefix(record.Namespace, "__kvstore_internal_") || record.Key == "" ||
		len(record.Namespace)+len(record.Key)+len(record.Value) > 256<<10 {
		return errors.New("invalid namespace, empty key or record exceeds the destination size budget")
	}
	if record.GetExpiresAtUnixMs() < 0 {
		return errors.New("invalid expiration deadline")
	}
	return nil
}
