ARG RUST_VERSION=1.82.0
ARG RUST_PROFILE
ARG RUSTFLAGS='-C target-cpu=native'
ARG RELEASE_TAG
ARG PYSPARK_VERSION=3.5.1

FROM python:3.11-slim AS builder

ARG TARGETPLATFORM
ARG RUST_VERSION
ARG RUST_PROFILE
ARG RUSTFLAGS
ARG RELEASE_TAG

ENV RUST_VERSION=${RUST_VERSION}
ENV RUST_PROFILE=${RUST_PROFILE}
ENV RUSTFLAGS=${RUSTFLAGS}
ENV RELEASE_TAG=${RELEASE_TAG}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        libc6-dev \
        protobuf-compiler \
        libprotobuf-dev \
        curl \
        git

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init && \
    chmod a+x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION}

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# This avoids expensive copies:
#   - Release builds have no build context.
#   - Dev builds use a local build context along with a .dockerignore file
COPY . .

RUN if [ -n "${RELEASE_TAG}" ]; then \
        echo "Fetching source code from GitHub at: ${RELEASE_TAG}" && \
        git clone https://github.com/lakehq/sail.git /tmp/sail && \
        git -C /tmp/sail checkout ${RELEASE_TAG} && \
        cp /tmp/sail/Cargo.toml . && \
        cp /tmp/sail/Cargo.lock . && \
        cp -r /tmp/sail/crates . && \
        echo "Contents:" && ls -la && \
        rm -rf /tmp/sail; \
    else \
        echo "Using local build context for source code"; \
    fi

RUN --mount=type=cache,target=/root/.cargo/registry,id=${TARGETPLATFORM} \
    --mount=type=cache,target=/root/.cargo/git,id=${TARGETPLATFORM} \
    --mount=type=cache,target=/app/target,id=${TARGETPLATFORM} \
    RUST_TARGET_SUBDIR=$(case "${RUST_PROFILE}" in \
        dev|test) echo "debug" ;; \
        release|prod|bench) echo "release" ;; \
        *) echo "${RUST_PROFILE}" ;; \
    esac) && \
    echo "RUSTFLAGS: ${RUSTFLAGS}" && \
    cargo build -p sail-cli --profile ${RUST_PROFILE} --bins && \
    cp /app/target/${RUST_TARGET_SUBDIR}/sail /usr/local/bin

FROM python:3.11-slim

ARG RUST_VERSION
ARG RUST_PROFILE
ARG RUSTFLAGS
ARG RELEASE_TAG
ARG PYSPARK_VERSION

ENV RUST_VERSION=${RUST_VERSION}
ENV RUST_PROFILE=${RUST_PROFILE}
ENV RUSTFLAGS=${RUSTFLAGS}
ENV RELEASE_TAG=${RELEASE_TAG}
ENV PYSPARK_VERSION=${PYSPARK_VERSION}

RUN python3 -m pip install --no-cache-dir "pyspark[connect]==${PYSPARK_VERSION}"

COPY --from=builder /usr/local/bin/sail /usr/local/bin

ENTRYPOINT ["/usr/local/bin/sail"]
