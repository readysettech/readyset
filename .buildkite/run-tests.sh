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
export CARGO_TERM_PROGRESS_WHEN=never

# If we aren't actually running run-tests.sh as a parallel job, just follow the
# "job 0" path.
: "${BUILDKITE_PARALLEL_JOB:=0}"

if [[ "$BUILDKITE_PARALLEL_JOB" == "0" ]]; then
    # Run nextest
    echo "+++ :rust: Run tests (nextest)"
    cargo --locked nextest run --profile ci --hide-progress-bar \
        --workspace --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        || upload_artifacts
elif [[ "$BUILDKITE_PARALLEL_JOB" == "1" ]]; then
    # Run doctests, because at this time nextest does not support doctests
    echo "+++ :rust: Run tests (doctest)"
    cargo --locked test --doc \
        --workspace --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        || upload_artifacts
else
    echo "No command defined for this parallel job."
    exit 1
fi
