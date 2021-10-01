#!/usr/bin/env bash
set -euo pipefail

upload_artifacts() {
    echo "--- Uploading proptest-regressions to buildkite artifacts"
    buildkite-agent artifact upload '**/proptest-regressions/*.txt'
    exit 1
}

echo "+++ :rust: Compile tests"
cargo --locked test --all --exclude clustertest --no-run
echo "+++ :rust: Run tests"
cargo --locked test --all --exclude clustertest -- --skip integration_serial || upload_artifacts
echo "+++ :rust: Compile serial integration tests"
cargo --locked test -p noria-server integration_serial --no-run
echo "+++ :rust: Run serial integration tests"
cargo --locked test -p noria-server integration_serial -- --test-threads=1 || upload_artifacts
