FROM rustlang/rust:nightly
RUN apt-get update
RUN apt-get install -y \
    clang \
    libclang-dev \
    libssl-dev \
    liblz4-dev \
    build-essential

RUN rustup toolchain install nightly-2020-11-15

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
