FROM mcr.microsoft.com/devcontainers/python:3.11

RUN --mount=type=bind,source=local/bootstrap,target=/tmp/bootstrap \
    if [ -f /tmp/bootstrap/run.sh ]; then \
        bash -l -c "/tmp/bootstrap/run.sh"; \
    fi

RUN --mount=type=bind,source=scripts/install-java.sh,target=/tmp/install-java.sh \
    bash -l -c "/tmp/install-java.sh"

RUN --mount=type=bind,source=scripts/install-node.sh,target=/tmp/install-node.sh \
    bash -l -c "/tmp/install-node.sh"

RUN --mount=type=bind,source=scripts/install-zig.sh,target=/tmp/install-zig.sh \
    bash -l -c "/tmp/install-zig.sh"

RUN --mount=type=bind,source=scripts/install-tools.sh,target=/tmp/install-tools.sh \
    bash -l -c "/tmp/install-tools.sh"

RUN --mount=type=bind,source=scripts/install-rust.sh,target=/tmp/install-rust.sh \
    bash -l -c "/tmp/install-rust.sh"

RUN --mount=type=bind,source=local/finalize,target=/tmp/finalize \
    if [ -f /tmp/finalize/run.sh ]; then \
        bash -l -c "/tmp/finalize/run.sh"; \
    fi
