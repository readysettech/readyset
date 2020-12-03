FROM rustlang/rust:nightly
RUN apt-get update
RUN apt-get install -y \
    clang \
    libclang-dev \
    libssl-dev \
    liblz4-dev \
    build-essential

RUN rustup toolchain install nightly-2020-11-15
