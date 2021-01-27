#!/usr/bin/env bash
set -xeuo pipefail

docker build -f build/Dockerfile --cache-from readyset-build:latest -t readyset-build:latest .
docker volume create readyset-target
docker volume create readyset-cargo
docker volume create readyset-sccache

docker run --rm --name readyset-build \
    -v "$(pwd):/app" \
    -v readyset-target:/app/target \
    -v readyset-cargo:/root/.cargo \
    -v readyset-sccache:/root/.cache/sccache \
    -e CARGO_INCREMENTAL=0 \
    -w /app \
    readyset-build:latest \
    sh -c 'cargo build --release --bin vote && ./target/release/vote --csv ./results.csv localsoup'

windtunnel-cli report -f csv < results.csv
