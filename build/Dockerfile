FROM public.ecr.aws/docker/library/rust:1.87.0

WORKDIR /tmp

RUN set -eux; \
    apt-get update && \
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
        rsync && \
    rm -rf /var/lib/apt/lists/*

COPY rust-toolchain.toml .

RUN rustup default "$(cat rust-toolchain.toml | grep channel | sed -e 's/.*"\(.*\)"/\1/')"; \
    rm rust-toolchain.toml
RUN rustup component add clippy rustfmt

RUN curl -L https://github.com/docker/compose/releases/download/v2.6.0/docker-compose-linux-`uname -m` \
      -o /usr/local/bin/docker-compose && \
    chmod a+x /usr/local/bin/docker-compose

RUN cargo install --locked \
      sccache \
      detect_flake \
      critcmp \
      cargo-deny
ENV RUSTC_WRAPPER=sccache

# Install cargo-nextest
RUN set -eux; \
    ARCH="$(uname -m)"; \
    VERSION="0.9.87"; \
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
        PLATFORM="linux-arm"; \
    elif [ "$ARCH" = "x86_64" ] || [ "$ARCH" = "amd64"]; then \
        PLATFORM="linux"; \
    else \
        echo "Unsupported architecture: $ARCH"; exit 1; \
    fi; \
    # CARGO_HOME is set by the nightly container to be /usr/local/cargo.
    # This is where it reads configs from for cargo and where cargo installs binaries.
    curl -LsSf "https://get.nexte.st/${VERSION}/${PLATFORM}" | tar zxf - -C ${CARGO_HOME}/bin

RUN printf '[build]\nrustflags = ["-C", "link-arg=-fuse-ld=lld"]' > ${CARGO_HOME}/config.toml
