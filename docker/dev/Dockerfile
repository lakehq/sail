FROM python:3.11-slim AS builder

ARG TARGETPLATFORM
ARG RUST_VERSION=1.82.0
ARG RUST_PROFILE=dev

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        libc6-dev \
        protobuf-compiler \
        libprotobuf-dev \
        curl

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init && \
    chmod a+x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION}

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY Cargo.toml .
COPY Cargo.lock .
COPY crates crates

RUN --mount=type=cache,target=/root/.cargo/registry,id=${TARGETPLATFORM} \
    --mount=type=cache,target=/root/.cargo/git,id=${TARGETPLATFORM} \
    --mount=type=cache,target=/app/target,id=${TARGETPLATFORM} \
    RUST_TARGET_SUBDIR=$(case "${RUST_PROFILE}" in \
        dev|test) echo "debug" ;; \
        release|bench) echo "release" ;; \
        *) echo "${RUST_PROFILE}" ;; \
    esac) && \
    cargo build -p sail-cli --profile ${RUST_PROFILE} --bins && \
    cp /app/target/${RUST_TARGET_SUBDIR}/sail /usr/local/bin

FROM python:3.11-slim

ARG PYSPARK_VERSION=3.5.1

RUN python3 -m pip install --no-cache-dir "pyspark[connect]==${PYSPARK_VERSION}"

COPY --from=builder /usr/local/bin/sail /usr/local/bin

ENTRYPOINT ["/usr/local/bin/sail"]
