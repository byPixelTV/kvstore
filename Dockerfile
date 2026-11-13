# syntax=docker/dockerfile:1.7

# This image builds the KVStore from Go source. It does NOT download a prebuilt KVStore.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS builder

WORKDIR /src

# Compatible with projects that do not have a go.sum yet.
COPY go.* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG BUILD_CHANNEL=stable

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    set -eu; \
    mkdir -p /out; \
    version="$(cat VERSION)"; \
    case "$BUILD_CHANNEL" in \
      stable) ;; \
      nightly) version="${version}-SNAPSHOT" ;; \
      *) echo "Invalid BUILD_CHANNEL: $BUILD_CHANNEL" >&2; exit 1 ;; \
    esac; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w -X main.version=$version" \
      -o /out/kvstore ./cmd/kvstore; \
    mkdir -p /out/data; \
    chown 65532:65532 /out/data

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/kvstore /usr/local/bin/kvstore
COPY --from=builder --chown=65532:65532 /out/data /data

USER 65532:65532
WORKDIR /data

ENV KVSTORE_LISTEN_ADDR=:50051 \
    KVSTORE_DATA_DIR=/data

EXPOSE 50051

ENTRYPOINT ["/usr/local/bin/kvstore"]
