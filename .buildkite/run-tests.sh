#!/usr/bin/env bash
set -euo pipefail

upload_artifacts() {
    echo "--- Uploading proptest-regressions to buildkite artifacts"
    buildkite-agent artifact upload '**/proptest-regressions/*.txt'
    buildkite-agent artifact upload '**/*.proptest-regressions'
    exit 1
}

enable_mrbr() {
    echo "+++ :mysql: Configure minimal row based replication"

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
}

export DISABLE_TELEMETRY=true
export PROPTEST_MAX_SHRINK_TIME=1800000
export CARGO_TERM_PROGRESS_WHEN=never

: "${TEST_CATEGORY:=nextest}"
: "${UPSTREAM_CONFIG:=default}"
: "${MYSQL_MRBR:=off}"

# We use $UPSTREAM_CONFIG to determine which tests to run; the appropriate docker containers and
# images should already have been set up by the pipeline step. The `default` config has mysql8,
# postgres15, postgres13, as well as consul and redis which are used for some non-upstream tests. We
# shouldn't need to filter down to a subset of the tests in that case. For specific upstreams, we
# run only the tests that run against that particular upstream.
#
# The filter sets should generally match the groups defined in .config/nextest.toml, but we can't
# just identify a group to run. Besides, down the line these tests may become parallelizable and we
# won't have need for nextest mutual exclusion groups (for all tests); for this reason, we don't
# here use the ':serial:` tag, even though it's used in the groups.
case "$UPSTREAM_CONFIG" in
  "none")
    NEXTEST_ARGS=(--ignore-default-filter -E "not test(/:(\w+)_upstream:/)") ;;
  mysql8*)
    # If we ever add a `mysql57_upstream` test, this will filter it out, and it can be added in the next case.
    NEXTEST_ARGS=(--ignore-default-filter -E "test(/:mysql8\d*_upstream:/) or test(/:mysql_upstream:/)") ;;
  "mysql57")
    NEXTEST_ARGS=(--ignore-default-filter -E "test(/:mysql_upstream:/)") ;;
  "postgres15")
    NEXTEST_ARGS=(--ignore-default-filter -E "test(/:postgres\d*_upstream:/)") ;;
  "postgres13")
    NEXTEST_ARGS=(--ignore-default-filter -E "test(/:postgres_upstream:/)") ;;
  *)
    NEXTEST_ARGS=() ;;
esac


if [[ "$TEST_CATEGORY" == "nextest" ]]; then
    if [[ "$MYSQL_MRBR" == "on" ]]; then
        if [[ "$UPSTREAM_CONFIG" != "mysql"* ]]; then
            echo "MRBR configured for non-MySQL upstream"
            exit 1
        fi
        enable_mrbr
    fi

    echo "+++ :rust: Run tests (nextest $UPSTREAM_CONFIG, MRBR $MYSQL_MRBR)"

    cargo --locked nextest run --profile ci --hide-progress-bar \
        --workspace --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        "${NEXTEST_ARGS[@]}" \
        || upload_artifacts
elif [[ "$TEST_CATEGORY" == "doctest" ]]; then
    # Run doctests, because at this time nextest does not support doctests
    echo "+++ :rust: Run tests (doctest)"
    cargo --locked test --doc \
        --workspace --features failure_injection \
        --exclude readyset-clustertest --exclude benchmarks \
        || upload_artifacts
else
    echo "No test defined for TEST_CATEGORY=${TEST_CATEGORY}."
    exit 1
fi
