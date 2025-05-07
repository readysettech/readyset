FROM public.ecr.aws/docker/library/rust:1.86.0

WORKDIR /tmp

COPY rust-toolchain.toml .

RUN rustup default "$(cat rust-toolchain.toml | grep channel | sed -e 's/.*"\(.*\)"/\1/')"; rm rust-toolchain.toml
RUN rustup component add clippy
RUN rustup component add rustfmt

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

RUN cargo install sccache --locked
ENV RUSTC_WRAPPER=sccache

RUN cargo install detect_flake --locked
RUN cargo install critcmp --locked
RUN cargo install cargo-deny --locked

# CARGO_HOME is set by the nightly container to be /usr/local/cargo.
# This is where it reads configs from for cargo and where cargo installs binaries.
RUN curl -LsSf https://get.nexte.st/0.9.87/linux | tar zxf - -C ${CARGO_HOME}/bin

RUN printf '[build]\nrustflags = ["-C", "link-arg=-fuse-ld=lld"]' > ${CARGO_HOME}/config.toml
