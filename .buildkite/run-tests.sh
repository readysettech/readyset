#!/usr/bin/env bash
set -euo pipefail

upload_junit() {
    local junit_dir="target/nextest/${NEXTEST_PROFILE}"
    local job_id="${BUILDKITE_JOB_ID:-local}"
    # Rename JUnit XML with unique suffix before uploading so the
    # junit-annotate plugin (which globs junit-*.xml) can find it.
    if [[ -f "${junit_dir}/junit.xml" ]]; then
        mv "${junit_dir}/junit.xml" \
           "${junit_dir}/junit-${job_id}.xml"
    fi
    # Upload any JUnit files that exist
    for f in "${junit_dir}"/junit*.xml; do
        if [[ -f "$f" ]]; then
            buildkite-agent artifact upload "$f"
        fi
    done
}

upload_artifacts() {
    upload_junit

    echo "--- Uploading proptest-regressions to buildkite artifacts"
    buildkite-agent artifact upload '**/proptest-regressions/*.txt'
    buildkite-agent artifact upload '**/*.proptest-regressions'
    exit 1
}

export DISABLE_TELEMETRY=true
export PROPTEST_MAX_SHRINK_TIME=600000
export CARGO_TERM_PROGRESS_WHEN=never

: "${TEST_CATEGORY:=nextest}"
: "${NEXTEST_PROFILE:=ci}"

if [[ "$TEST_CATEGORY" == "nextest" ]]; then
    NEXTEST_ARGS=(
        --config-file ../hacks/nextest-tcv/nextest.toml
        --profile "$NEXTEST_PROFILE"
        --hide-progress-bar
        --workspace --features failure_injection
    )

    if [[ "${MYSQL_MRBR:-off}" == "on" ]]; then
        echo "+++ :rust: Run tests (MySQL MRBR on)"
        NEXTEST_ARGS+=(--filterset 'test(/:mysql\d+:/)')
    elif [[ -n "${NEXTEST_FILTERSET:-}" ]]; then
        echo "+++ :rust: Run tests (nextest, filterset: $NEXTEST_FILTERSET)"
        NEXTEST_ARGS+=(--filterset "$NEXTEST_FILTERSET")
    else
        echo "+++ :rust: Run tests (nextest)"
    fi

    # Skip mysql57 on ARM — no native ARM64 image, QEMU emulation is unreliable
    if [[ "$(uname -m)" == "aarch64" ]]; then
        echo "--- Excluding mysql57 tests (no native ARM64 image)"
        NEXTEST_ARGS+=(--filterset 'not test(/:mysql57:/)')
    fi

    set -x
    cargo --locked nextest run "${NEXTEST_ARGS[@]}" || upload_artifacts
    set +x

    upload_junit

elif [[ "$TEST_CATEGORY" == "doctest" ]]; then
    echo "+++ :rust: Run tests (doctest)"
    set -x
    cargo --locked test --doc \
        --workspace --features failure_injection \
        || upload_artifacts
    set +x
else
    echo "No test defined for TEST_CATEGORY=${TEST_CATEGORY}."
    exit 1
fi
