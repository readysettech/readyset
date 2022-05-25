ARG READYSET_RUST_UBUNTU2004_BUILDER_BASE_TAG=latest
FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/readyset-rust-ubuntu2004-builder-base:${READYSET_RUST_UBUNTU2004_BUILDER_BASE_TAG} as builder

ENV CARGO_INCREMENTAL=0

ARG CACHEPOT_BUCKET
ARG CACHEPOT_REGION

WORKDIR /tmp
COPY . readyset

WORKDIR /tmp/readyset
RUN set -eux; \
    cargo --locked build -p readyset-substrate

FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/ubuntu:20.04 as fetcher

RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        curl \
        unzip; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

FROM fetcher as terraform-fetcher

RUN set -eux; \
    curl -o terraform.zip https://releases.hashicorp.com/terraform/1.1.6/terraform_1.1.6_linux_amd64.zip; \
    echo "3e330ce4c8c0434cdd79fe04ed6f6e28e72db44c47ae50d01c342c8a2b05d331 terraform.zip" | sha256sum -c -; \
    unzip terraform.zip

FROM fetcher as sops-fetcher

RUN set -eux; \
    curl -o sops -L https://github.com/mozilla/sops/releases/download/v3.7.1/sops-v3.7.1.linux; \
    echo "185348fd77fc160d5bdf3cd20ecbc796163504fd3df196d7cb29000773657b74 sops" | sha256sum -c; \
    chmod a+x sops

FROM fetcher as substrate-fetcher

RUN set -eux; \
    curl -o substrate.tar.gz https://src-bin.com/substrate-2022.05-20e34d8-linux-amd64.tar.gz; \
    mkdir -p substrate; \
    tar -C substrate --strip-components=1 -xvf substrate.tar.gz

FROM fetcher as awscli-fetcher

RUN set -eux; \
    curl -o "awscliv2.zip" "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.2.32.zip"; \
    unzip awscliv2.zip; \
    mkdir aws-cli-bin; \
    ./aws/install --bin-dir aws-cli-bin

FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/ubuntu:20.04 as runtime
WORKDIR /workdir
ENV SUBSTRATE_ROOT /workdir/ops/substrate

COPY --from=builder /tmp/readyset/target/debug/readyset-substrate /usr/local/bin/readyset-substrate
COPY --from=terraform-fetcher /tmp/terraform /usr/local/bin/
COPY --from=sops-fetcher /tmp/sops /usr/local/bin/
COPY --from=substrate-fetcher /tmp/substrate/ /usr/local/bin/
COPY --from=awscli-fetcher /usr/local/aws-cli/ /usr/local/aws-cli/
COPY --from=awscli-fetcher /tmp/aws-cli-bin/ /usr/local/bin/

RUN set -eux; \
    # Install minimal dependencies
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        git; \
    rm -rf /var/lib/apt/lists/*; \
    # Validate all programs are installed correctly
    terraform --version; \
    sops --version; \
    aws --version; \
    substrate --version; \
    readyset-substrate --help

ENTRYPOINT [ "readyset-substrate" ]
