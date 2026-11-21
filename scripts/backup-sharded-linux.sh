#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

if [[ $# != 3 || ! "$1" =~ ^(backup|restore)$ || ! "$2" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo 'Usage: backup-sharded-linux.sh backup|restore COMPOSE_PROJECT BACKUP_DIRECTORY' >&2
  exit 2
fi

action=$1
project=$2
archive=$3
helper=${KVSTORE_BACKUP_HELPER_IMAGE:-debian:bookworm-slim}
volumes=()
files=()
for shard in {0..2}; do
  for replica in {0..2}; do
    volumes+=("s${shard}n${replica}-data")
    files+=("s${shard}n${replica}-data.tar.gz")
  done
done

check_volume() {
  local volume="${project}_$1"
  local labels active
  labels=$(docker volume inspect --format '{{index .Labels "com.docker.compose.project"}}:{{index .Labels "com.docker.compose.volume"}}' "$volume")
  [[ "$labels" == "$project:$1" ]] || { echo "Unexpected ownership for volume $volume" >&2; exit 1; }
  active=$(docker ps --quiet --filter "volume=$volume")
  [[ -z "$active" ]] || { echo "Stop all containers using $volume first" >&2; exit 1; }
}

run_helper() {
  local volume="${project}_$1" mount_option=$2
  shift 2
  docker run --rm -i --network none --read-only --user 65532:65532 \
    --cap-drop ALL --security-opt no-new-privileges \
    --mount "type=volume,src=$volume,dst=/data,volume-nocopy$mount_option" \
    "$helper" "$@"
}

for volume in "${volumes[@]}"; do check_volume "$volume"; done

if [[ "$action" == backup ]]; then
  [[ ! -e "$archive" && ! -L "$archive" ]] || { echo 'Backup directory already exists' >&2; exit 1; }
  # Leave failed captures without a manifest so they cannot be restored.
  mkdir -- "$archive"
  cd -- "$archive"
  for i in "${!volumes[@]}"; do
    volume=${volumes[$i]}
    check_volume "$volume"
    run_helper "$volume" ',readonly' sh -ec 'test -d /data/state && test -d /data/raft/wal'
    run_helper "$volume" ',readonly' tar -czf - -C /data . > "${files[$i]}"
  done
  for volume in "${volumes[@]}"; do check_volume "$volume"; done
  sha256sum -- "${files[@]}" > SHA256SUMS.tmp
  sync -f .
  mv -- SHA256SUMS.tmp SHA256SUMS
  sync -f .
  echo "Backup completed for $project"
else
  cd -- "$archive"
  sha256sum -- "${files[@]}" | cmp -s - SHA256SUMS ||
    { echo 'Backup manifest or archive checksum mismatch' >&2; exit 1; }
  for volume in "${volumes[@]}"; do
    run_helper "$volume" ',readonly' sh -ec 'test "$(stat -c %u /data)" = 65532; test -z "$(ls -A /data)"'
  done
  for i in "${!volumes[@]}"; do
    volume=${volumes[$i]}
    check_volume "$volume"
    run_helper "$volume" '' tar -xzf - -C /data --no-same-owner < "${files[$i]}"
  done
  # Flush the restored data before the target can be started.
  for volume in "${volumes[@]}"; do
    check_volume "$volume"
    run_helper "$volume" '' sync -f /data
  done
  echo "Restore completed for $project. Validate the data before allowing clients."
fi
