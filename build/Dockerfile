FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/rust:nightly

RUN rustup toolchain install nightly-2021-03-01

ADD https://github.com/mozilla/sccache/releases/download/v0.2.15/sccache-v0.2.15-x86_64-unknown-linux-musl.tar.gz \
    sccache-v0.2.15-x86_64-unknown-linux-musl.tar.gz

RUN tar \
    -xzf sccache-v0.2.15-x86_64-unknown-linux-musl.tar.gz \
    --strip-components=1 \
    -C /bin \
    sccache-v0.2.15-x86_64-unknown-linux-musl/sccache

RUN chmod a+x /bin/sccache

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
