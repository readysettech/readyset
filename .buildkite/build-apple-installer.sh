#!/bin/sh
set -eu

rustup target add x86_64-apple-darwin
echo "+++ Running build for x86_64-apple-darwin"
RUSTFLAGS='-D warnings' cargo build --release --bin readyset-installer --target=x86_64-apple-darwin
echo "+++ Running build for aarch64-apple-darwin"
RUSTFLAGS='-D warnings' cargo build --release --bin readyset-installer --target=aarch64-apple-darwin
echo "+++ Stripping binaries"
strip target/x86_64-apple-darwin/release/readyset-installer
strip target/aarch64-apple-darwin/release/readyset-installer
