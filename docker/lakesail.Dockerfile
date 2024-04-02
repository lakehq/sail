# syntax=docker/dockerfile:1
FROM rust:1.76.0-bookworm AS builder

ARG TARGETPLATFORM

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    ca-certificates \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*

# TODO: See if this is necessary
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=${TARGETPLATFORM} \
    cargo install \
    cargo-strip

# TODO: .dockerignore file
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=${TARGETPLATFORM} \
    --mount=type=cache,target=/app/target,id=${TARGETPLATFORM} \
    cargo build --release && \
    cargo strip && \
    mv /app/target/release/spark-connect-server /app


FROM debian:bookworm-slim

ENV RUST_LOG=debug

# TODO: See if ca-certificates and python3-dev are needed in the final image
RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*

ENV LAKESAIL_OPENTELEMETRY_COLLECTOR="1"

# TODO: Adjust once we have a proper entrypoint
COPY --from=builder /app/spark-connect-server /
ENTRYPOINT ["./spark-connect-server"]
