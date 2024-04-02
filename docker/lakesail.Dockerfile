# syntax=docker/dockerfile:1
FROM rust:1.76.0-bookworm AS builder

ARG TARGETPLATFORM

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

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

RUN apt-get update && \
    apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# TODO: Adjust once we have a proper entrypoint
COPY --from=builder /app/spark-connect-server /
ENTRYPOINT ["./spark-connect-server"]