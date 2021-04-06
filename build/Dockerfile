FROM rustlang/rust:nightly

RUN rustup toolchain install nightly-2021-01-01

ADD https://github.com/mozilla/sccache/releases/download/0.2.13/sccache-0.2.13-x86_64-unknown-linux-musl.tar.gz \
    sccache-0.2.13-x86_64-unknown-linux-musl.tar.gz

RUN tar \
    -xzf sccache-0.2.13-x86_64-unknown-linux-musl.tar.gz \
    --strip-components=1 \
    -C /bin \
    sccache-0.2.13-x86_64-unknown-linux-musl/sccache

RUN mkdir -p ~/.cargo
ENV RUSTC_WRAPPER /bin/sccache
RUN printf '[build]\nrustc-wrapper=/bin/sccache\n' > ~/.cargo/config

RUN apt-get update
RUN apt-get install -y \
    clang \
    cmake \
    libclang-dev \
    libssl-dev \
    liblz4-dev \
    librdkafka-dev \
    build-essential \
    llvm \
    rsync

ENV CXX /bin/sccache c++

RUN mkdir -p /root/.sccache
