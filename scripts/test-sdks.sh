#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:?Pass the SDK version to test}
npm_output=${2:?Pass an absolute output directory for the verified npm tarball}
maven_version="$version"
if [[ "$version" == *-SNAPSHOT.* ]]; then
  maven_version="${version%%-SNAPSHOT.*}-SNAPSHOT"
fi
mkdir -p "$npm_output"
fixture="$(mktemp -d)"
server_pid=
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" || true
  fi
  rm -rf "$fixture"
}
trap cleanup EXIT

go build -o "$fixture/kvstore" ./cmd/kvstore
export KVSTORE_SDK_TEST_TARGET="127.0.0.1:51671"
export KVSTORE_SDK_TEST_KEY
KVSTORE_SDK_TEST_KEY="$("$fixture/kvstore" apikey create --dir "$fixture/keys" --name sdk --read --write)"
KVSTORE_MODE=standalone KVSTORE_LISTEN_ADDR="$KVSTORE_SDK_TEST_TARGET" \
  KVSTORE_DATA_DIR="$fixture/data" KVSTORE_API_KEYS_DIR="$fixture/keys" \
  "$fixture/kvstore" >"$fixture/server.log" 2>&1 &
server_pid=$!
ready=false
for _ in {1..50}; do
  if KVSTORE_HEALTH_ADDR="$KVSTORE_SDK_TEST_TARGET" "$fixture/kvstore" healthcheck >/dev/null 2>&1; then
    ready=true
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then break; fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  cat "$fixture/server.log" >&2
  exit 1
fi

go test -count=1 ./sdk/go
(
  cd sdk/kotlin
  bash ./gradlew --no-daemon "-PsdkVersion=$maven_version" test publishMavenPublicationToVerificationRepository
)
package_source="$fixture/source/sdk/typescript"
proto_source="$fixture/source/api/proto/kvstore/v1"
mkdir -p "$package_source" "$proto_source"
cp -R sdk/typescript/{src,README.md,copy-proto.mjs,package.json,package-lock.json,tsconfig.json} "$package_source/"
cp api/proto/kvstore/v1/kvstore.proto "$proto_source/"
(
  cd "$package_source"
  npm ci
  npm version "$version" --no-git-tag-version --allow-same-version
  npm pack --pack-destination "$fixture"
)
mkdir "$fixture/consumer"
cp sdk/typescript/test/client.test.mjs "$fixture/consumer/"
(
  cd "$fixture/consumer"
  npm install --ignore-scripts "$fixture"/*.tgz
  node --test client.test.mjs
)
cp "$fixture"/*.tgz "$npm_output/"
