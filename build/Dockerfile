FROM public.ecr.aws/docker/library/rust:1.93.0

ARG TARGETARCH

# If not using buildx with --platform, TARGETARCH will be undefined and the build will fail.  Assume
# x86 as the default in that case.
ENV TARGETARCH=${TARGETARCH:-amd64}

WORKDIR /tmp

RUN set -eux; \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        curl \
        docker.io \
        jq \
        libclang-dev \
        liblz4-dev \
        libssl-dev \
        lld \
        llvm \
        mariadb-client \
        postgresql-client \
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

# Install cargo-binstall for faster binary installation
RUN curl -L --proto '=https' --tlsv1.2 -sSf \
      "https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh" | bash

# Install tools via cargo-binstall
RUN cargo binstall -y --locked \
      detect_flake \
      cargo-deny \
      cargo-nextest@0.9.124 \
      sccache@0.13.0
ENV RUSTC_WRAPPER=sccache

# tcv pinned version should match public/build/docker-compose.ci-tcv.yml
COPY --from=305232526136.dkr.ecr.us-east-2.amazonaws.com/test-container-vendor:87c75e439d6169669fd06f036c328e8df873cbee /usr/local/bin/tcv /usr/local/bin/tcv

# Set some rustflags.  For info on the graviton flag, see:
#   https://github.com/aws/aws-graviton-getting-started/blob/main/rust.md
RUN linker_flag='"-C", "link-arg=-fuse-ld=lld"' \
    && graviton_flag='"-C", "target-feature=+lse"' \
    && if [ "${TARGETARCH}" = "arm64" ]; then \
         rustflags="${linker_flag}, ${graviton_flag}"; \
       else \
         rustflags="${linker_flag}"; \
       fi \
    && printf '[build]\nrustflags = [%s]\n' "${rustflags}" > ${CARGO_HOME}/config.toml
