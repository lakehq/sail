FROM rust:1.76.0-bookworm AS builder

WORKDIR /app

RUN apt-get update

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update

COPY --from=builder /app/target/release/hello /usr/local/bin
ENTRYPOINT ["/usr/local/bin/hello"]
