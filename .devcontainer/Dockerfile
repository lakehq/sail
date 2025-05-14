# Base image: Python 3.11 with devcontainer support
FROM mcr.microsoft.com/devcontainers/python:3.11

ARG ZIG_VERSION=0.14.0
ARG PYSPARK_VERSION=3.5.4

# Install Zig (required for Rust build tooling or other native extensions)

RUN curl -fsSL https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_VERSION}.tar.xz | \
    tar -xJf - -C /opt/ && \
    ln -s /opt/zig-${ZIG_VERSION}/zig /usr/local/bin/zig

# Install base Python tools: Hatch and Maturin (PySpark is managed via Hatch envs)
RUN pip install --upgrade pip && \
    pip install hatch maturin

# Set the working directory inside the container
WORKDIR /workspace
