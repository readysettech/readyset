FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/rustlang/rust:nightly as chef
WORKDIR /workdir
COPY rust-toolchain ./rust-toolchain
RUN rustup toolchain install "$(cat ./rust-toolchain)"
RUN cargo install cargo-chef
RUN apt-get update
RUN apt-get install -y \
    clang \
    cmake \
    libclang-dev \
    libssl-dev \
    liblz4-dev \
    librdkafka-dev \
    build-essential \
    llvm

FROM chef as planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
WORKDIR /workdir
COPY rust-toolchain ./rust-toolchain
RUN rustup toolchain install "$(cat ./rust-toolchain)"
RUN cargo install cargo-chef
COPY --from=planner /workdir/recipe.json recipe.json
COPY --from=planner $CARGO_HOME $CARGO_HOME
RUN cargo chef cook --check -p readyset-substrate --recipe-path recipe.json
COPY . .
RUN cargo --offline build -p readyset-substrate

FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/rust:1.54 as fetcher
WORKDIR /workdir

RUN set -eux; \
    curl -o terraform.zip https://releases.hashicorp.com/terraform/1.0.2/terraform_1.0.2_linux_amd64.zip; \
    echo "7329f887cc5a5bda4bedaec59c439a4af7ea0465f83e3c1b0f4d04951e1181f4 terraform.zip" | sha256sum -c -; \
    unzip -d /usr/local/bin terraform.zip

RUN set -eux; \
    curl -o /usr/local/bin/sops -L https://github.com/mozilla/sops/releases/download/v3.7.1/sops-v3.7.1.linux; \
    echo "185348fd77fc160d5bdf3cd20ecbc796163504fd3df196d7cb29000773657b74 /usr/local/bin/sops" | sha256sum -c; \
    chmod a+x /usr/local/bin/sops

FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/rust:1.54 as runtime
WORKDIR /workdir
ENV SUBSTRATE_ROOT /workdir/ops/substrate

COPY --from=builder /workdir/target/debug/readyset-substrate /usr/local/bin/readyset-substrate
COPY --from=fetcher /usr/local/bin/* /usr/local/bin/

RUN set -eux; \
    curl -o substrate.tar.gz https://src-bin.com/substrate-2021.09-1174b29-linux-amd64.tar.gz; \
    tar -C /usr/local/bin --strip-components=1 -xvf substrate.tar.gz

RUN set -eux; \
    curl -o "awscliv2.zip" "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.2.32.zip"; \
    unzip awscliv2.zip; \
    ./aws/install -i /usr/local/aws-cli -b /usr/local/bin

ENTRYPOINT [ "readyset-substrate" ]
