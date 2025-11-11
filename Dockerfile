FROM rust:1.83-bookworm AS builder

ARG CARGO_PROFILE=release
ARG RUSTFLAGS="--cfg tokio_unstable"

RUN apt-get update && apt-get install -y \
    ca-certificates \
    clang \
    cmake \
    libssl-dev \
    llvm \
    pkg-config \
    libprotobuf-dev \
    protobuf-compiler \
    libsasl2-dev \
    libzstd-dev \
    liblz4-dev \
    libclang-dev \
    build-essential \
    nasm \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    RUSTFLAGS="$RUSTFLAGS" cargo build \
    -p openwit-cli \
    --bin openwit-cli \
    $(test "$CARGO_PROFILE" = "release" && echo "--release") && \
    cp target/${CARGO_PROFILE}/openwit-cli /openwit-cli

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    libzstd1 \
    liblz4-1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /openwit

COPY --from=builder /openwit-cli /usr/local/bin/openwit
COPY config/openwit-unified-control.yaml.example /openwit/config/default.yaml

RUN mkdir -p /openwit/data /openwit/config && \
    chmod +x /usr/local/bin/openwit

ENV RUST_LOG=info \
    OPENWIT_CONFIG_PATH=/openwit/config/default.yaml \
    OPENWIT_DATA_DIR=/openwit/data

EXPOSE 7019 8080 8081 8083 4318 50051 9401

VOLUME ["/openwit/data", "/openwit/config"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:7019/health || exit 1

ENTRYPOINT ["/usr/local/bin/openwit"]
CMD ["--help"]
