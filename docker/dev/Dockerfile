ARG RUST_VERSION=1.88.0
ARG RUST_PROFILE=dev
ARG RUSTFLAGS=""
ARG PYSPARK_VERSION=4.0.0

FROM python:3.11-slim AS builder

ARG RUST_VERSION
ARG RUST_PROFILE
ARG RUSTFLAGS

ENV RUSTFLAGS="${RUSTFLAGS}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        libc6-dev \
        protobuf-compiler \
        libprotobuf-dev \
        curl \
        git \
        pkg-config

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init && \
    chmod a+x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION}

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Use the local repository as the build context.
# The .dockerignore file at the project root directory skips unnecessary files.
COPY . .

RUN --mount=type=cache,target=/root/.cargo/registry/ \
    --mount=type=cache,target=/root/.cargo/git/ \
    --mount=type=cache,target=/app/target/ \
    RUST_TARGET_SUBDIR=$(case "${RUST_PROFILE}" in \
        dev|test) echo "debug" ;; \
        release|bench) echo "release" ;; \
        *) echo "${RUST_PROFILE}" ;; \
    esac) && \
    cargo build -p sail-cli --profile ${RUST_PROFILE} --bins && \
    cp /app/target/${RUST_TARGET_SUBDIR}/sail /usr/local/bin

FROM python:3.11-slim

ARG PYSPARK_VERSION

RUN python3 -m pip install --no-cache-dir "pyspark[connect]==${PYSPARK_VERSION}"

COPY --from=builder /usr/local/bin/sail /usr/local/bin

ENTRYPOINT ["/usr/local/bin/sail"]
