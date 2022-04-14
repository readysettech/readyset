#!/bin/sh
set -eux

rustup target add x86_64-apple-darwin
RUSTFLAGS='-D warnings' cargo build --release --bin readyset-installer --target=x86_64-apple-darwin
RUSTFLAGS='-D warnings' cargo build --release --bin readyset-installer --target=aarch64-apple-darwin
strip target/x86_64-apple-darwin/release/readyset-installer
strip target/aarch64-apple-darwin/release/readyset-installer