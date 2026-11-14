#!/usr/bin/env bash
set -Eeuo pipefail

# Usage:
#   ./scripts/cleanup-docker.sh benchmarks
#   ./scripts/cleanup-docker.sh all
#   ./scripts/cleanup-docker.sh all --volumes
#   ./scripts/cleanup-docker.sh benchmarks --dry-run
#   ./scripts/cleanup-docker.sh all --dry-run
#
# "benchmarks" removes Docker resources labelled with a Docker Compose
# project whose name starts with "kvbench-".
# "all" removes ALL containers, including production containers.
# --volumes additionally removes ALL named Docker volumes in "all" mode.

mode="${1:-benchmarks}"
shift $(( $# > 0 ? 1 : 0 ))
remove_volumes=false
dry_run=false
for option in "$@"; do
  case "$option" in
    --volumes) remove_volumes=true ;;
    --dry-run) dry_run=true ;;
    *) echo "Unbekannte Option: $option" >&2; exit 2 ;;
  esac
done

case "$mode" in
  benchmarks)
    if $remove_volumes; then
      echo "--volumes ist nur im Modus 'all' nötig (Benchmarks entfernen Projekt-Volumes automatisch)." >&2
      exit 2
    fi
    ;;
  all) ;;
  *)
    echo "Aufruf: $0 [benchmarks|all] [--volumes] [--dry-run]" >&2
    exit 2
    ;;
esac

command -v docker >/dev/null || { echo "Docker fehlt." >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker-Daemon nicht erreichbar." >&2; exit 1; }

run() {
  if $dry_run; then
    printf '+ '
    printf '%q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

if [[ "$mode" == "all" ]]; then
  mapfile -t containers < <(docker ps -aq)
  mapfile -t volumes < <(docker volume ls -q)

  echo "ACHTUNG: Dieser Modus entfernt ALLE ${#containers[@]} Docker-Container, auch PRODUKTIVE!"
  echo "Container-Dateisysteme und anonyme Volumes werden gelöscht. Images bleiben erhalten."
  if $remove_volumes; then
    echo "ZUSÄTZLICH werden ALLE ${#volumes[@]} benannten Docker-Volumes gelöscht (Datenbanken, Welten usw.)!"
  else
    echo "Benannte Volumes bleiben erhalten. Mit '--volumes' würden auch sie gelöscht."
  fi

  if ! $dry_run; then
    [[ -t 0 ]] || { echo "Nur interaktiv ausführbar. Kein Cleanup durchgeführt." >&2; exit 1; }
    expected="DELETE ALL CONTAINERS"
    if $remove_volumes; then expected="DELETE ALL DOCKER DATA"; fi
    read -r -p "Zur Bestätigung exakt '$expected' eingeben: " confirmation
    [[ "$confirmation" == "$expected" ]] || { echo "Abgebrochen."; exit 1; }
  fi

  if ((${#containers[@]} > 0)); then
    run docker rm -f -v "${containers[@]}"
  fi
  if $remove_volumes && ((${#volumes[@]} > 0)); then
    run docker volume rm "${volumes[@]}"
  fi
  echo "Fertig."
  exit 0
fi

# Identify all kvbench-* Compose projects across containers, networks, and volumes;
# resource labels allow cleanup even after containers have already stopped.
mapfile -t projects < <(
  {
    docker ps -a --filter 'label=com.docker.compose.project' --format '{{.Label "com.docker.compose.project"}}'
    docker network ls -q --filter 'label=com.docker.compose.project' | while IFS= read -r id; do
      docker network inspect -f '{{index .Labels "com.docker.compose.project"}}' "$id"
    done
    docker volume ls -q --filter 'label=com.docker.compose.project' | while IFS= read -r name; do
      docker volume inspect -f '{{index .Labels "com.docker.compose.project"}}' "$name"
    done
  } | grep '^kvbench-' | LC_ALL=C sort -u || true
)

if ((${#projects[@]} == 0)); then
  echo "Keine kvbench-* Docker-Compose-Projekte gefunden."
  exit 0
fi

printf 'Benchmark-Projekte (%s):\n' "${#projects[@]}"
printf '  %s\n' "${projects[@]}"

for project in "${projects[@]}"; do
  echo "== Entferne $project =="
  mapfile -t containers < <(docker ps -aq --filter "label=com.docker.compose.project=$project")
  mapfile -t networks < <(docker network ls -q --filter "label=com.docker.compose.project=$project")
  mapfile -t volumes < <(docker volume ls -q --filter "label=com.docker.compose.project=$project")

  # Delete containers first so attached volumes and networks can be removed.
  if ((${#containers[@]} > 0)); then run docker rm -f -v "${containers[@]}"; fi
  if ((${#networks[@]} > 0)); then run docker network rm "${networks[@]}"; fi
  if ((${#volumes[@]} > 0)); then run docker volume rm "${volumes[@]}"; fi
done

echo "Benchmark-Cleanup abgeschlossen. Andere Docker-Projekte wurden nicht angerührt."
