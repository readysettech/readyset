#!/usr/bin/env bash
set -euo pipefail

upload_artifacts() {
    echo "--- Uploading proptest-regressions to buildkite artifacts"
    buildkite-agent artifact upload '**/proptest-regressions/*.txt'
    buildkite-agent artifact upload '**/*.proptest-regressions'
    exit 1
}

export DISABLE_TELEMETRY=true
export PROPTEST_MAX_SHRINK_TIME=1800000

# If we aren't actually running run-tests.sh as a parallel job, just follow the
# "job 0" path.
: "${BUILDKITE_PARALLEL_JOB:=0}"

if [[ "$BUILDKITE_PARALLEL_JOB" == "0" ]]; then
    # Run nextest
    echo "+++ :rust: Run tests (nextest)"
    exit 1
    cargo --locked nextest run --workspace \
        --ignore-default-filter --hide-progress-bar \
        --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        || upload_artifacts
elif [[ "$BUILDKITE_PARALLEL_JOB" == "1" ]]; then
    exit 0
    # Run doctests, because at this time nextest does not support doctests
    echo "+++ :rust: Run tests (doctest)"
    cargo --locked test --workspace \
        --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        --doc \
        || upload_artifacts
else
    echo "No command defined for this parallel job."
    exit 1
fi
