FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/rustlang/rust:nightly

WORKDIR /tmp

COPY rust-toolchain .

RUN rustup default "$(cat public/rust-toolchain)"; rm rust-toolchain

RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        docker.io \
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
        protobuf-compiler \
        rsync; \
    rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/docker/compose/releases/download/v2.6.0/docker-compose-linux-`uname -m` -o /usr/local/bin/docker-compose; \
    chmod a+x /usr/local/bin/docker-compose

RUN set -eux; \
    cargo install detect_flake; \
    cargo install critcmp;

RUN cargo install sccache --locked
ENV RUSTC_WRAPPER=sccache

# CARGO_HOME is set by the nightly container to be /usr/local/cargo.
# This is where it reads configs from for cargo and where cargo installs binaries.
RUN printf '[build]\nrustflags = ["-C", "link-arg=-fuse-ld=lld"]' > ${CARGO_HOME}/config
