# syntax=docker/dockerfile:1.7
# Builds from the checked-in Protobuf bindings and locked Go dependencies.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS builder
WORKDIR /src

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG BUILD_CHANNEL=stable

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    set -eu; \
    version="$(cat VERSION)"; \
    case "$BUILD_CHANNEL" in \
      stable) ;; \
      nightly) version="${version}-SNAPSHOT" ;; \
      *) echo "Unknown BUILD_CHANNEL: $BUILD_CHANNEL" >&2; exit 1 ;; \
    esac; \
    mkdir -p /out/data; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
      go build -mod=readonly -trimpath -ldflags="-s -w -X main.version=$version" \
      -o /out/kvstore ./cmd/kvstore; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
      go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/kvbench ./cmd/kvbench; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
      go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/kvshardctl ./cmd/kvshardctl; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
      go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/kvimport ./cmd/kvimport; \
    chown 65532:65532 /out/data

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/kvstore /usr/local/bin/kvstore
COPY --from=builder /out/kvbench /usr/local/bin/kvbench
COPY --from=builder /out/kvshardctl /usr/local/bin/kvshardctl
COPY --from=builder /out/kvimport /usr/local/bin/kvimport
COPY --from=builder --chown=65532:65532 /out/data /data
USER 65532:65532
WORKDIR /data
ENV KVSTORE_MODE=standalone \
    KVSTORE_LISTEN_ADDR=:50051 \
    KVSTORE_HEALTH_ADDR=127.0.0.1:50051 \
    KVSTORE_DATA_DIR=/data \
    KVSTORE_SHUTDOWN_TIMEOUT=15s
EXPOSE 50051 7000 7001 7002
ENTRYPOINT ["/usr/local/bin/kvstore"]
