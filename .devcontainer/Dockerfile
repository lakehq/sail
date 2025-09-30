FROM mcr.microsoft.com/devcontainers/python:3.11

RUN --mount=type=bind,source=scripts/local,target=/tmp/local \
    if [ -f /tmp/local/bootstrap.sh ]; then \
        /tmp/local/bootstrap.sh; \
    fi

RUN --mount=type=bind,source=scripts/install-java.sh,target=/tmp/install-java.sh \
    bash -i -c "/tmp/install-java.sh"

RUN --mount=type=bind,source=scripts/install-node.sh,target=/tmp/install-node.sh \
    bash -i -c "/tmp/install-node.sh"

RUN --mount=type=bind,source=scripts/install-zig.sh,target=/tmp/install-zig.sh \
    bash -i -c "/tmp/install-zig.sh"

RUN --mount=type=bind,source=scripts/install-tools.sh,target=/tmp/install-tools.sh \
    bash -i -c "/tmp/install-tools.sh"

RUN --mount=type=bind,source=scripts/install-rust.sh,target=/tmp/install-rust.sh \
    bash -i -c "/tmp/install-rust.sh"
