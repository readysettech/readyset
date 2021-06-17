FROM rust AS builder

ADD src/Cargo.toml /src/
WORKDIR /src
# Cache a layer with all the dependencies built
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build && rm -Rvf src
ADD src/src /src/src
RUN touch src/main.rs && cargo build

FROM fedora:32
COPY --from=builder /src/target/debug/mysql-test /
ENTRYPOINT ["/mysql-test"]

