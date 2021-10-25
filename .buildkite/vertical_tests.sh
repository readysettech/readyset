#!/usr/bin/env bash
set -euo pipefail

upload_regressions() {
    echo "--- Uploading proptest-regressions to buildkite artifacts"
    buildkite-agent artifact upload noria-mysql/tests/vertical.proptest-regressions
    exit 1
}

echo '+++ Compiling vertical tests'
cargo --locked test -p noria-mysql --features vertical_tests --test vertical --no-run
echo '+++ Running vertical tests'
cargo --locked test -p noria-mysql --features vertical_tests --test vertical || upload_regressions
