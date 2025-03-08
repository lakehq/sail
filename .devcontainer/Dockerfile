FROM mcr.microsoft.com/devcontainers/python:3.10


ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG PYSPARK_VERSION=3.5.4

ENV ZIG_VERSION=0.14.0
ENV NODE_VERSION=v22.0.0

# Install required packages
RUN apt-get update && apt-get install -y \
    software-properties-common \
    make \
    curl \
    zip \
    unzip \
    tar \
    git \
    wget \
    ca-certificates \
    gcc \
    libc6-dev \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Zig (Latest Stable Version)
RUN curl -fsSL https://ziglang.org/builds/zig-linux-x86_64-$ZIG_VERSION.tar.xz | tar -xvJf - -C /usr/local/bin


RUN wget -O - https://apt.corretto.aws/corretto.key | sudo gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list

RUN apt-get update; sudo apt-get install -y java-17-amazon-corretto-jdk
USER $USERNAME
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y 
# Ensure the path is set for rust
ENV PATH="/home/$USERNAME/.cargo/bin:$NVM_DIR/versions/node/${NODE_VERSION}/bin:/usr/local/bin:$PATH"
RUN rustup toolchain install stable --component rustfmt  && rustup toolchain install nightly --component rustfmt 
RUN cargo install maturin  

# Install NVM & Node.js 22
ENV NVM_DIR="/home/$USERNAME/.nvm"
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash && \
    . "$NVM_DIR/nvm.sh" && nvm install 22 && nvm use 22 
RUN wget -qO- https://get.pnpm.io/install.sh | ENV="$HOME/.bashrc" SHELL="$(which bash)" bash -


# Install Maturin (Python package), Hatch, and Protocol Buffers
RUN pip install --upgrade pip \
    && pip install maturin hatch  
WORKDIR /workspace

    # Set working directory
ARG PYSPARK_VERSION=3.5.4
RUN python3 -m pip install --no-cache-dir "pyspark[connect]==${PYSPARK_VERSION}"

#RUN cargo build -p sail-cli --profile ${RUST_PROFILE} --bins && \
#    cp /app/target/${RUST_TARGET_SUBDIR}/sail /usr/local/bin

WORKDIR /workspace
