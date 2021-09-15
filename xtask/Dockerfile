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
RUN cargo chef cook --check -p xtask --recipe-path recipe.json
COPY . .
RUN cargo --offline build -p xtask

FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/rust:1.54 as runtime
WORKDIR /workdir
ENV SUBSTRATE_ROOT /workdir/ops/substrate

COPY --from=builder /workdir/target/debug/xtask /usr/local/bin/xtask
ENTRYPOINT [ "xtask" ]
