FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/rustlang/rust:nightly

WORKDIR /tmp

COPY rust-toolchain .

RUN rustup toolchain install "$(cat rust-toolchain)"; rm rust-toolchain

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
        curl \
        rsync; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    cargo install cargo-cache --version 0.6.3; \
    cargo install --git https://github.com/paritytech/cachepot --rev 2c4b14f4164ae954a1e6241e14c13feada0f1758; \
    cargo cache --remove-dir all,git-db; \
    mkdir -p ~/.config/cachepot; touch ~/.config/cachepot/config # To clean up cachepot logs

# CARGO_HOME is set by the nightly container to be /usr/local/cargo.
# This is where it reads configs from for cargo and where cargo installs binaries.
RUN printf '[build]\nrustc-wrapper="/usr/local/cargo/bin/cachepot"\nrustflags = ["-C", "link-arg=-fuse-ld=lld"]' > ${CARGO_HOME}/config

RUN set -eux; \
    curl -LO https://github.com/readysettech/flaky-finder/releases/download/v0.0.1/flaky-finder-v0.0.1-x86_64-linux.tar.gz; \
    tar xf flaky-finder-v0.0.1-x86_64-linux.tar.gz -C /usr/local/bin; \
    rm flaky-finder-v0.0.1-x86_64-linux.tar.gz; \
    chmod a+x /usr/local/bin/flaky-finder; \
    flaky-finder --version

