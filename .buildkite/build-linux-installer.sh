#!/bin/sh
set -eux

rustup target add x86_64-unknown-linux-musl
RUSTFLAGS='-D warnings' cargo build --release --bin readyset-installer --target x86_64-unknown-linux-musl
strip target/x86_64-unknown-linux-musl/release/readyset-installer