# Base image: Python 3.11 with devcontainer support
FROM mcr.microsoft.com/devcontainers/python:3.11

ARG ZIG_VERSION=0.14.0

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    gcc \
    git \
    krb5-user \
    libc6-dev \
    libgssapi-krb5-2 \
    libprotobuf-dev \
    pkg-config \
    protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Install Zig (required for Rust build tooling or other native extensions)
RUN curl -fsSL https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_VERSION}.tar.xz | \
    tar -xJf - -C /opt/ && \
    ln -s /opt/zig-${ZIG_VERSION}/zig /usr/local/bin/zig

# Install Python tools
RUN pip install --upgrade pip && \
    pip install hatch maturin
