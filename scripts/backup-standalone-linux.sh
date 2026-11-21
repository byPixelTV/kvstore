#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
export LC_ALL=C
unset CDPATH TAR_OPTIONS GZIP

if [[ $# != 3 || ! "$1" =~ ^(backup|restore)$ || ! "$2" =~ ^[a-z0-9][a-z0-9_-]*$ || -z "$3" ]]; then
  echo 'Usage: backup-standalone-linux.sh backup|restore COMPOSE_PROJECT BACKUP_DIRECTORY' >&2
  exit 2
fi

action=$1
project=$2
archive=$3
helper=${KVSTORE_BACKUP_HELPER_IMAGE:-debian:bookworm-slim}
archive_file=kvstore-data.tar.gz
format=kvstore-standalone-volume-v1

fail() { echo "$*" >&2; exit 1; }

# Resolve the volume from the actual service mount, including custom Compose names.
container=$(docker container ls --all --quiet \
  --filter "label=com.docker.compose.project=$project" \
  --filter 'label=com.docker.compose.service=kvstore' \
  --filter 'label=com.docker.compose.oneoff=False')
[[ "$container" =~ ^[a-f0-9]+$ ]] || fail "Expected one created kvstore service container in $project"
mode=$(docker container inspect --format '{{range .Config.Env}}{{if eq . "KVSTORE_MODE=standalone"}}standalone{{end}}{{end}}' "$container")
data_dir=$(docker container inspect --format '{{range .Config.Env}}{{if eq . "KVSTORE_DATA_DIR=/data"}}/data{{end}}{{end}}' "$container")
[[ "$mode" == standalone && "$data_dir" == /data ]] || fail "Expected standalone KVStore with data in /data"
volume=$(docker container inspect --format '{{range .Mounts}}{{if and (eq .Destination "/data") (eq .Type "volume") .RW}}{{println .Name}}{{end}}{{end}}' "$container")
[[ "$volume" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]] || fail "Expected a writable named volume at /data"
ownership=$(docker volume inspect --format '{{index .Labels "com.docker.compose.project"}}:{{index .Labels "com.docker.compose.volume"}}:{{.Driver}}:{{len .Options}}' "$volume")
[[ "$ownership" == "$project:kvstore-data:local:0" ]] || fail "Expected a Compose-owned local kvstore-data volume for $project"

check_stopped() {
  local state active
  state=$(docker container inspect --format '{{.State.Status}}' "$container")
  [[ "$state" == created || "$state" == exited ]] || fail "Stop the kvstore service in $project first"
  active=$(docker container ls --quiet --filter "volume=$volume")
  [[ -z "$active" ]] || fail "Stop all containers using $volume first"
}

run_helper() {
  local mount_option=$1
  shift
  # The per-volume name prevents overlapping helpers from passing each other's checks.
  docker run --rm -i --name "${volume}-kvstore-offline-backup" --network none --read-only \
    --user 65532:65532 --cap-drop ALL --security-opt no-new-privileges \
    --mount "type=volume,src=$volume,dst=/data,volume-nocopy$mount_option" \
    "$helper" "$@"
}

validate_archive() {
  # GNU tar emits escaped member names. The standalone database contains only
  # regular files and directories with simple names, all owned by the service.
  tar --list --verbose --gzip --numeric-owner --quoting-style=escape --file="$archive_file" |
    awk '
      {
        path = $6
        sub(/\/$/, "", path)
        if (NF != 6 || length($1) != 10 || $1 !~ /^[-d][rwx-]+$/ || $2 != "65532/65532" || path in types) exit 1
        if (path == "." && $6 == "./" && substr($1, 1, 1) == "d") {
          types[path] = "d"
          next
        }
        if (path !~ /^\.\/([a-zA-Z0-9._-]+\/)*[a-zA-Z0-9._-]+$/ || path ~ /\/\.\.?($|\/)/) exit 1
        parent = path
        sub(/\/[^/]+$/, "", parent)
        if (types[parent] != "d") exit 1
        types[path] = substr($1, 1, 1)
        if (types[path] == "-" && $3 > 0 && path ~ /^\.\/MANIFEST-[0-9]+$/) manifest = 1
      }
      END { if (!manifest) exit 1 }
    ' || fail 'Backup archive has invalid entries or lacks a Pebble manifest'
}

check_stopped
if [[ "$action" == backup ]]; then
  mkdir -m 700 -- "$archive" || fail 'Backup requires a new directory in an existing parent'
  cd -- "$archive"
  check_stopped
  run_helper ',readonly' tar --create --gzip --format=gnu --hard-dereference --numeric-owner \
    --one-file-system --file=- --directory=/data . > "$archive_file"
  check_stopped
  validate_archive
  printf '%s\n' "$format" > FORMAT
  # Publish the checksums only after a complete, validated capture.
  sha256sum -- FORMAT "$archive_file" > SHA256SUMS.tmp
  sync -f .
  mv -- SHA256SUMS.tmp SHA256SUMS
  sync -f .
  echo "Standalone backup completed for $project"
else
  while [[ "$archive" == */ && "$archive" != / ]]; do archive=${archive%/}; done
  [[ -d "$archive" && ! -L "$archive" ]] || fail 'Expected a standalone backup directory'
  cd -- "$archive"
  for file in FORMAT "$archive_file" SHA256SUMS; do
    [[ -f "$file" && ! -L "$file" ]] || fail "Missing or invalid backup file: $file"
  done
  printf '%s\n' "$format" | cmp -s - FORMAT || fail 'Unsupported standalone backup format'
  sha256sum -- FORMAT "$archive_file" | cmp -s - SHA256SUMS || fail 'Backup manifest or archive checksum mismatch'
  validate_archive
  check_stopped
  run_helper '' sh -ec '
    [ "$(stat -c "%u:%g" /data)" = "65532:65532" ] && [ -w /data ] && [ -x /data ] ||
      { echo "Restore target is not owned and writable by the KVStore user" >&2; exit 1; }
    entries=$(find /data -mindepth 1 -maxdepth 1 -printf x -quit)
    [ -z "$entries" ] || { echo "Restore target is not empty" >&2; exit 1; }
    tar --extract --gzip --file=- --directory=/data --no-same-owner --same-permissions --keep-old-files
    sync -f /data
  ' < "$archive_file"
  check_stopped
  echo "Standalone restore completed for $project. Validate the data before allowing clients."
fi
