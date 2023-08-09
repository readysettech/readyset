#!/usr/bin/env bash
set -euo pipefail

upload_artifacts() {
    echo "--- Uploading proptest-regressions to buildkite artifacts"
    buildkite-agent artifact upload '**/proptest-regressions/*.txt'
    buildkite-agent artifact upload '**/*.proptest-regressions'
    exit 1
}

cd public
echo "+++ :rust: Run tests"
export DISABLE_TELEMETRY=true
cargo --locked test --all --features failure_injection --exclude readyset-clustertest || upload_artifacts
