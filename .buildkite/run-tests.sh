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

# Default to "nextest" run:
: "${MATRIX_VALUE:=nextest}"

if [[ "$MATRIX_VALUE" == "nextest" ]]; then
    # Run nextest
    echo "+++ :rust: Run tests (nextest)"
    cargo --locked nextest run --profile ci --hide-progress-bar \
        --workspace --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        || upload_artifacts
elif [[ "$MATRIX_VALUE" == "doctest" ]]; then
    # Run doctests, because at this time nextest does not support doctests
    echo "+++ :rust: Run tests (doctest)"
    cargo --locked test --doc \
        --workspace --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        || upload_artifacts
elif [[ "$MATRIX_VALUE" == "MRBR" ]]; then
    echo "+++ :rust: Run tests (mysql minimal row-based replication)"

    # Need a mysql-client to set binlog_row_image:
    if ! command -v mysql &> /dev/null; then
        echo "'mysql' command not found. Installing MySQL client..."
        apt-get update
        apt-get install -y --no-install-recommends default-mysql-client
    fi

    # For minimal row based replication testing, we must set `binlog_row_image=MINIMAL` on the mysql
    # database.  However, it might not be up yet, so attempt it in a retry loop:
    max_attempts=10
    attempt=1
    interval=3
    while [ $attempt -le $max_attempts ]; do
        if (mysql -h mysql --password=noria -e "SET GLOBAL binlog_row_image=MINIMAL;"); then
            echo "mysql 'SET GLOBAL binlog_row_image=MINIMAL' succeeded."
            break
        else
            echo "mysql 'SET GLOBAL binlog_row_image=MINIMAL' failed.  Retrying..."
            attempt=$((attempt + 1))
            sleep $interval
        fi
    done
    if [ $attempt -gt $max_attempts ]; then
        echo "mysql 'SET GLOBAL binlog_row_image=MINIMAL' failed after $max_attempts attempts."
        exit 1
    fi

    cargo --locked nextest run --profile ci --hide-progress-bar \
        --workspace --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        --ignore-default-filter -E 'test(~serial_mysql)' \
        || upload_artifacts
else
    echo "No test defined for MATRIX_VALUE=${MATRIX_VALUE}."
    exit 1
fi
