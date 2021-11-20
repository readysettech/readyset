FROM ubuntu:20.04

WORKDIR /tmp

RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        llvm \
        clang \
        libclang-dev \
        lld \
        cmake \
        libssl-dev \
        liblz4-dev \
        pkg-config \
        curl \
        rsync; \
    rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

COPY rust-toolchain .

RUN set -eux; \
    url="https://static.rust-lang.org/rustup/archive/1.24.3/x86_64-unknown-linux-gnu/rustup-init"; \
    curl -O "$url"; \
    echo "3dc5ef50861ee18657f9db2eeb7392f9c2a6c95c90ab41e45ab4ca71476b4338 *rustup-init" | sha256sum -c -; \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain $(cat rust-toolchain) --default-host x86_64-unknown-linux-gnu; \
    rm rustup-init rust-toolchain; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    rustup --version; \
    cargo --version; \
    rustc --version

RUN set -eux; \
    cargo install cargo-cache --version 0.6.3; \
    cargo install --git https://github.com/paritytech/cachepot --rev 2c4b14f4164ae954a1e6241e14c13feada0f1758; \
    cargo cache --remove-dir all,git-db; \
    mkdir -p ~/.config/cachepot; touch ~/.config/cachepot/config # To clean up cachepot logs

# CARGO_HOME is set by the nightly container to be /usr/local/cargo.
# This is where it reads configs from for cargo and where cargo installs binaries.
RUN printf '[build]\nrustc-wrapper="/usr/local/cargo/bin/cachepot"\nrustflags = ["-C", "link-arg=-fuse-ld=lld"]' > ${CARGO_HOME}/config
