#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
export LC_ALL=C
unset CDPATH TAR_OPTIONS GZIP

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$root"
backup="$root/scripts/backup-standalone-linux.sh"
image=${KVSTORE_TEST_IMAGE:-kvstore:dev}
helper=${KVSTORE_BACKUP_HELPER_IMAGE:-debian:bookworm-slim}
fail() { echo "$*" >&2; exit 1; }

[[ "$(docker info --format '{{.OSType}}')" == linux ]] || fail 'An isolated Linux Docker daemon is required'
docker image inspect "$image" >/dev/null || fail 'Build the standalone image or set KVSTORE_TEST_IMAGE to a local image'

out=$(mktemp -d)
out=$(realpath -e -- "$out")
suffix=$(od -An -N12 -tx1 /dev/urandom | tr -d '[:space:]')
source_project="kvstore-backup-$suffix-source"
target_project="kvstore-backup-$suffix-target"
holder="kvstore-backup-$suffix-holder"
export KVSTORE_TEST_IMAGE=$image

compose() {
  local project=$1
  shift
  KVSTORE_TEST_VOLUME="$project-volume" docker compose -p "$project" -f "$root/compose.yml" -f "$out/override.yml" "$@"
}

cleanup() {
  local rc=$?
  trap - EXIT
  if [[ -n "$holder" ]] && docker container inspect "$holder" >/dev/null 2>&1; then
    docker container rm --force "$holder" >/dev/null || rc=1
  fi
  if [[ -f "$out/override.yml" ]]; then
    if [[ "$rc" != 0 ]]; then
      compose "$source_project" logs --tail 15 >&2 || true
      compose "$target_project" logs --tail 15 >&2 || true
    fi
    compose "$source_project" down --volumes --timeout 3 >/dev/null || rc=1
    compose "$target_project" down --volumes --timeout 3 >/dev/null || rc=1
  fi
  [[ -d "$out" && -O "$out" && "$out" == "$(realpath -e -- "$out")" ]] && rm -rf -- "$out"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cat > "$out/override.yml" <<'YAML'
services:
  kvstore:
    image: ${KVSTORE_TEST_IMAGE:?}
    restart: 'no'
    ports: !override ['127.0.0.1::50051']
volumes:
  kvstore-data:
    name: ${KVSTORE_TEST_VOLUME:?}
YAML

cat > "$out/probe.go" <<'GO'
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	pb "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func must[T any](value T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return value
}

func main() {
	const namespace = "backup-check"
	value := []byte{0, 0xff, 0x80, '\n', 0x01}
	conn := must(grpc.NewClient(os.Args[2], grpc.WithTransportCredentials(insecure.NewCredentials())))
	defer conn.Close()
	client := pb.NewKVStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	must(client.Head(ctx, &pb.HeadRequest{}, grpc.WaitForReady(true)))
	if os.Args[1] == "seed" {
		must(client.Set(ctx, &pb.SetRequest{Namespace: namespace, Key: "removed", Value: []byte("removed")}))
		must(client.Delete(ctx, &pb.DeleteRequest{Namespace: namespace, Key: "removed"}))
		must(client.Set(ctx, &pb.SetRequest{Namespace: namespace, Key: "current", Value: []byte("old")}))
		must(client.Set(ctx, &pb.SetRequest{
			Namespace: namespace, Key: "current", Value: value, ExpiresAtUnixMs: time.Now().Add(time.Hour).UnixMilli(),
		}))
	}
	head := must(client.Head(ctx, &pb.HeadRequest{}))
	record := must(client.Get(ctx, &pb.GetRequest{Namespace: namespace, Key: "current"}))
	removed := must(client.Get(ctx, &pb.GetRequest{Namespace: namespace, Key: "removed"}))
	if head.Revision != 4 || !record.Found || record.Revision != 4 || !bytes.Equal(record.Value, value) ||
		record.ExpiresAtUnixMs <= time.Now().UnixMilli() || removed.Found {
		log.Fatalf("unexpected standalone state: %v, %v, %v", head, record, removed)
	}
	watch := must(client.Watch(ctx, &pb.WatchRequest{Namespace: namespace, Key: "current", AfterRevision: 3}))
	event := must(watch.Recv())
	if event.Revision != 4 || !bytes.Equal(event.Value, value) || event.ExpiresAtUnixMs != record.ExpiresAtUnixMs {
		log.Fatalf("unexpected persisted change event: %v", event)
	}
	result := struct {
		Head   *pb.HeadResponse
		Record *pb.GetResponse
		Event  *pb.WatchResponse
	}{head, record, event}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		log.Fatal(err)
	}
}
GO

expect_rejected() {
  local reason=$1
  shift
  if bash "$backup" "$@" > "$out/rejection.log" 2>&1; then
    fail "Expected $1 to fail: $reason"
  fi
  if ! grep -F -- "$reason" "$out/rejection.log" >/dev/null; then
    cat -- "$out/rejection.log" >&2
    fail "Unexpected error from $1, expected $reason"
  fi
}

go build -mod=readonly -o "$out/probe" "$out/probe.go"
compose "$source_project" up -d --no-build --pull never
"$out/probe" seed "$(compose "$source_project" port kvstore 50051)" > "$out/before.json"
expect_rejected 'Stop the kvstore service' backup "$source_project" "$out/backup"
[[ ! -e "$out/backup" ]] || fail 'Backup created output for an active source'
compose "$source_project" stop kvstore

mkdir -- "$out/existing"
expect_rejected 'Backup requires a new directory' backup "$source_project" "$out/existing"
bash "$backup" backup "$source_project" "$out/backup"
[[ "$(stat -c %a "$out/backup")" == 700 ]] || fail 'Backup directory permissions are not private'
[[ "$(stat -c %a "$out/backup/kvstore-data.tar.gz")" == 600 ]] || fail 'Archive permissions are not private'

expect_rejected 'Expected one created kvstore service container' restore "$target_project" "$out/backup"
compose "$target_project" create --no-build --pull never kvstore
docker run --rm -d --name "$holder" --network none --read-only --user 65532:65532 \
  --cap-drop ALL --security-opt no-new-privileges \
  --mount "type=volume,src=$target_project-volume,dst=/data,volume-nocopy,readonly" \
  "$helper" sleep 300 >/dev/null
expect_rejected 'Stop all containers using' restore "$target_project" "$out/backup"
docker container stop "$holder" >/dev/null
holder=

mkdir -- "$out/partial" "$out/unsafe" "$out/payload"
cp -- "$out/backup/FORMAT" "$out/backup/kvstore-data.tar.gz" "$out/partial/"
expect_rejected 'Missing or invalid backup file: SHA256SUMS' restore "$target_project" "$out/partial"
cp -a -- "$out/backup" "$out/tampered"
printf 'tampered' >> "$out/tampered/kvstore-data.tar.gz"
expect_rejected 'Backup manifest or archive checksum mismatch' restore "$target_project" "$out/tampered"
cp -- "$out/backup/FORMAT" "$out/unsafe/"
printf 'test manifest\n' > "$out/payload/MANIFEST-000001"
ln -s -- /etc/passwd "$out/payload/unsafe-link"
(
  cd -- "$out/unsafe"
  tar --create --gzip --format=gnu --owner=65532 --group=65532 --file=kvstore-data.tar.gz --directory="$out/payload" .
  sha256sum -- FORMAT kvstore-data.tar.gz > SHA256SUMS
)
expect_rejected 'Backup archive has invalid entries' restore "$target_project" "$out/unsafe"

bash "$backup" restore "$target_project" "$out/backup"
expect_rejected 'Restore target is not empty' restore "$target_project" "$out/backup"
compose "$target_project" start kvstore
"$out/probe" check "$(compose "$target_project" port kvstore 50051)" > "$out/after.json"
cmp -- "$out/before.json" "$out/after.json"
echo 'standalone backup/restore checks passed'
