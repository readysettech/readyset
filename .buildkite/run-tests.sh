#!/usr/bin/env bash
set -euo pipefail

upload_artifacts() {
    echo "--- Uploading proptest-regressions to buildkite artifacts"
    buildkite-agent artifact upload '**/proptest-regressions/*.txt'
    buildkite-agent artifact upload '**/*.proptest-regressions'
    exit 1
}

echo "+++ :rust: Run tests"
export DISABLE_TELEMETRY=true
LOG_LEVEL=info cargo --locked test -p readyset-client types::enums --format=json --exact -Z unstable-options --show-output
