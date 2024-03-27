FROM rust:1.76.0-bookworm AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates

RUN cargo build --release

FROM debian:bookworm-slim

ENV RUST_LOG=trace

RUN apt-get update && \
    apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# TODO: Adjust once we have a proper entrypoint
COPY --from=builder /app/target/release/spark-connect-server /usr/local/bin

EXPOSE 50051

ENTRYPOINT ["/usr/local/bin/spark-connect-server"]
