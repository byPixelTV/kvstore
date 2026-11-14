# syntax=docker/dockerfile:1.7
# Compiles KVStore from source, including generated Protobuf bindings.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS builder
WORKDIR /src

# The Protobuf compiler is a build-time tool only; none of it is in runtime.
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
ENV PATH="/go/bin:${PATH}"

COPY go.mod ./
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG BUILD_CHANNEL=stable

RUN protoc -I=api/proto \
      --go_out=. --go_opt=module=github.com/byPixelTV/kvstore \
      --go-grpc_out=. --go-grpc_opt=module=github.com/byPixelTV/kvstore \
      api/proto/kvstore/v1/kvstore.proto

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    set -eu; \
    go mod tidy; \
    version="$(cat VERSION)"; \
    case "$BUILD_CHANNEL" in \
      stable) ;; \
      nightly) version="${version}-SNAPSHOT" ;; \
      *) echo "Unknown BUILD_CHANNEL: $BUILD_CHANNEL" >&2; exit 1 ;; \
    esac; \
    mkdir -p /out/data; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
      go build -trimpath -ldflags="-s -w -X main.version=$version" \
      -o /out/kvstore ./cmd/kvstore; \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
      go build -trimpath -ldflags="-s -w" -o /out/kvbench ./cmd/kvbench; \
    chown 65532:65532 /out/data

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/kvstore /usr/local/bin/kvstore
COPY --from=builder /out/kvbench /usr/local/bin/kvbench
COPY --from=builder --chown=65532:65532 /out/data /data
USER 65532:65532
WORKDIR /data
ENV KVSTORE_MODE=standalone \
    KVSTORE_LISTEN_ADDR=:50051 \
    KVSTORE_HEALTH_ADDR=127.0.0.1:50051 \
    KVSTORE_DATA_DIR=/data \
    KVSTORE_SHUTDOWN_TIMEOUT=15s
EXPOSE 50051 7000 7001
ENTRYPOINT ["/usr/local/bin/kvstore"]
