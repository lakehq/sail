FROM rust:1.76.0-bookworm AS builder

WORKDIR /app

RUN apt-get update

COPY crates .
COPY Cargo.toml .
COPY Cargo.lock .

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update

COPY --from=builder /app/target/release/hello /usr/local/bin
ENTRYPOINT ["/usr/local/bin/hello"]
