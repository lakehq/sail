# syntax=docker/dockerfile:1
FROM rust:1.76.0-bookworm AS builder

ARG TARGETPLATFORM

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    ca-certificates \
    python3-dev \
    python3-pip \
    python3-venv && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark
ARG PYSPARK_VERSION=3.5.1
ENV PYSPARK_VERSION $PYSPARK_VERSION
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN python3 -m pip install "pyspark[connect,sql]==${PYSPARK_VERSION}" # TODO: Look into only capturing pyspark.cloudpickle

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

ENV LAKESAIL_OPENTELEMETRY_COLLECTOR="1"

# TODO: See if ca-certificates needed in the final image for opentelemetry
RUN apt-get update && \
    apt-get install -y \
    ca-certificates  \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy the Python venv from builder with PySpark
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# TODO: Adjust once we have a proper entrypoint
COPY --from=builder /app/spark-connect-server /
ENTRYPOINT ["./spark-connect-server"]
