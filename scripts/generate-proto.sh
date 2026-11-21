#!/usr/bin/env bash
set -Eeuo pipefail

# Place this file in scripts/ of the KVStore repository.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

for command in go protoc; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Error: '$command' is not installed or not in PATH." >&2
    exit 1
  fi
done

# The Go protoc plugins are commonly installed under $(go env GOPATH)/bin.
GO_BIN="$(go env GOPATH)/bin"
export PATH="$GO_BIN:$PATH"
for command in protoc-gen-go protoc-gen-go-grpc; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Error: '$command' is missing. Install it with:" >&2
    if [[ "$command" == "protoc-gen-go" ]]; then
      echo '  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest' >&2
    else
      echo '  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest' >&2
    fi
    exit 1
  fi
done

protoc -I=api/proto \
  --go_out=. \
  --go_opt=module=github.com/byPixelTV/kvstore \
  --go-grpc_out=. \
  --go-grpc_opt=module=github.com/byPixelTV/kvstore \
  api/proto/kvstore/v1/*.proto

echo 'Generated Go protobuf bindings under gen/go/kvstore/v1'
