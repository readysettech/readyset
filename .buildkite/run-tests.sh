#!/usr/bin/env bash
set -euo pipefail

upload_artifacts() {
    echo "--- Uploading proptest-regressions to buildkite artifacts"
    buildkite-agent artifact upload '**/proptest-regressions/*.txt'
    exit 1
}

echo "+++ :rust: Running tests"
cargo test --all --exclude clustertest -- --skip integration_serial || upload_artifacts
echo "+++ :rust: Running serial integration tests"
cargo test -p noria-server integration_serial -- --test-threads=1 || upload_artifacts
