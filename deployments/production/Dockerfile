ARG RUST_VERSION=1.98.1
ARG RUST_PROFILE=release
ARG RUSTFLAGS=""
ARG PYSPARK_VERSION=4.2.0

FROM python:3.14-slim AS builder

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
        pkg-config && \
    rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init && \
    chmod a+x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile default --default-toolchain "${RUST_VERSION}"

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# The repository .dockerignore limits the context to Cargo manifests and crates.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN --mount=type=cache,target=/root/.cargo/registry/ \
    --mount=type=cache,target=/root/.cargo/git/ \
    --mount=type=cache,target=/app/target/ \
    RUST_TARGET_SUBDIR=$(case "${RUST_PROFILE}" in \
        dev|test) echo "debug" ;; \
        release|bench) echo "release" ;; \
        *) echo "${RUST_PROFILE}" ;; \
    esac) && \
    cargo build -p sail-cli --profile "${RUST_PROFILE}" --bins && \
    cp "/app/target/${RUST_TARGET_SUBDIR}/sail" /usr/local/bin/sail

FROM python:3.14-slim

ARG PYSPARK_VERSION
ARG SOURCE_REVISION="unknown"

LABEL org.opencontainers.image.title="Sail" \
      org.opencontainers.image.source="https://github.com/lakehq/sail" \
      org.opencontainers.image.revision="${SOURCE_REVISION}"

RUN python3 -m pip install --root-user-action=ignore --no-cache-dir \
      "pyspark-client==${PYSPARK_VERSION}" \
      "pandas<3.0.0" && \
    groupadd --gid 10001 sail && \
    useradd --uid 10001 --gid 10001 --no-create-home \
      --home-dir /tmp/sail-home --shell /usr/sbin/nologin sail

COPY --from=builder /usr/local/bin/sail /usr/local/bin/sail

ENV HOME="/tmp/sail-home" \
    PYTHONDONTWRITEBYTECODE="1" \
    PYTHONPYCACHEPREFIX="/tmp/python-cache"

USER 10001:10001

# The Spark Connect server shuts down gracefully on SIGINT. `sail worker` has no
# signal handler. Container runtimes use this image setting for normal stops.
# Active-request behavior still requires a workload-specific termination test.
STOPSIGNAL SIGINT

ENTRYPOINT ["/usr/local/bin/sail"]
