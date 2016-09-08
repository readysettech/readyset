# distributary
Soup, a structured storage system with automatic materialization and
feed-forward dataflow.

## Setup
You need nightly Rust to run this code.

An easy way of installing is to use `rustup.rs`:
```
$ curl https://sh.rustup.rs -sSf | sh
```

## Build & run benchmark
```
$ cargo test
$ cargo build --release
$ target/release/bench_votes
```
