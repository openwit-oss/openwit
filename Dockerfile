FROM rust:bookworm AS bin-builder

ARG CARGO_PROFILE=release
ARG CARGO_LOG=plain
ARG GH_PAT

# Set environment variables
ENV GH_PAT=$GH_PAT

# Install all dependencies including those needed for rdkafka and aws-lc
RUN apt-get -y update \
    && apt-get -y install -y \
        ca-certificates \
        clang \
        cmake \
        libssl-dev \
        llvm \
        python3.11 \
        python3-pip \
        pkg-config \
        libprotobuf-dev \
        protobuf-compiler \
        libsasl2-dev \
        libzstd-dev \
        liblz4-dev \
        libclang-dev \
        build-essential \
        libc6-dev \
        nasm \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /openwit

# Copy workspace definition first for better caching
COPY Cargo.toml /openwit/
COPY src/ /openwit/src/
COPY config/openwit-unified-control.yaml /openwit/config/default.yaml

RUN rustup toolchain install
RUN git config --global url."https://$GH_PAT@github.com/".insteadOf "https://github.com/"

# Build the actual binary - don't specify target, build for native platform
RUN echo "Building workspace for profile '$CARGO_PROFILE'"

# Build without cross-compilation
RUN RUSTFLAGS="--cfg tokio_unstable" \
    cargo build -p openwit-cli --bin openwit-cli \
    $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    $(test "$CARGO_LOG" = "verbose" && echo "--verbose")

RUN echo "Copying binaries to /openwit/bin"

# Create binary output directory and move binaries there
RUN mkdir -p /openwit/bin && find ./target/$CARGO_PROFILE -maxdepth 1 -perm /a+x -type f -exec mv {} /openwit/bin \;

# Final stage
FROM debian:bookworm-slim AS openwit

RUN apt-get -y update \
    && apt-get -y install -y \
        ca-certificates \
        libssl3 \
        libsasl2-2 \
        libzstd1 \
        liblz4-1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /openwit

# Expose common ports
EXPOSE 7280 7460 7470 7480 7490 7500 9090 9401 8081 4317 4318 50051

# Set environment variables
ENV OPENWIT_CONFIG_PATH=/openwit/config/default.yaml
ENV OPENWIT_DATA_DIR=/openwit/data
ENV RUST_LOG=info

# Copy binary and default configuration from the builder stage
COPY --from=bin-builder /openwit/bin/openwit-cli /usr/local/bin/openwit
COPY --from=bin-builder /openwit/config/default.yaml /openwit/config/default.yaml

# Volume for persistent data
VOLUME ["/openwit/data", "/openwit/config"]