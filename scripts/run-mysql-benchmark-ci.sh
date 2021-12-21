#!/bin/bash
set -euf -x -o pipefail

# The purpose of this script is to run each of our required benchmarks
# serially, so we can get a clear picture of how Noria performs under
# a variety of load patterns.
#
# The original intent for this process is for it to be executed inside
# the benchmarks service within the following docker-compose file, in CI:
# .buildkite/docker-compose.readyset-benchmark-mysql80.yml


# ----------- [Constants] ----------------------------------- #

BENCH_LOG_START_MSG='Benchmark starting.'
BENCH_LOG_SUCCESS_MSG='Benchmark completed successfully!'
BENCH_LOG_FAILURE_MSG='Benchmark failed to execute!'

# Location of YAML benchmark definitions
BENCHMARK_FILE_DIR=${BENCHMARK_FILE_DIR:-/usr/src/app/src/yaml/benchmarks/test}
BENCHMARK_EAGER_EXIT="${BENCHMARK_EAGER_EXIT:-'enabled'}"
REPL_AWAIT_SLEEP_DURATION=${REPL_AWAIT_SLEEP_DURATION:-120}
# Benchmark duration controls, overridable from env vars in calling code
FASTLY_READ_BENCHMARK_DURATION=${FASTLY_READ_BENCHMARK_DURATION:-120}

# Used to calculate total duration
BENCHMARK_START_TIME=$(date +%s)

# ----------- [Functions] ----------------------------------- #

log_benchmark () {
    project=$1
    bench_f=$2
    msg=$3
    printf "[%s - %s] %s" "${project}" "${bench_f}" "${msg}"
}

check_benchmark_exit_code () {
    local code=$1
    local project=$2
    local bench_f=$3

    if [ $code -gt 0 ]; then
        # Error, so handle it
        log_benchmark "${project}" "${bench_f}" "${BENCH_LOG_FAILURE_MSG}"
    else
        # Happy path
        log_benchmark "${project}" "${bench_f}" "${BENCH_LOG_SUCCESS_MSG}"
        return
    fi

    # Figure out if we need to fail fast
    if [ "${BENCHMARK_EAGER_EXIT}" == "enabled" ]; then
        m="Eager exit is enabled. Short circuiting tests now..."
        log_benchmark "${project}" "${bench_f}" "${m}"
        exit 1
    fi

    m="Eager exiting disabled. Continuing with other tests."
    log_benchmark "${project}" "${bench_f}" "${m}"
}

# ----------- [Main] ---------------------------------------- #

echo 'Beginning execution of ReadySet MySQL benchmarks'

# TODO: @Marcus - swap this out for a more robust "snapshots completed" approach.
echo "[SHIM]: Sleeping for 600s to let snapshot complete."
sleep 600

echo 'Beginning execution of ReadySet MySQL benchmarks'

# --------------------------------------------
# IRL Small Read Benchmark
# --------------------------------------------
bench_file='read_benchmark_irl_small.yaml'
project='IRL'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}" "1"

benchmarks \
    --skip-setup \
    --benchmark "$BENCHMARK_FILE_DIR/$bench_file"

check_benchmark_exit_code $? "${project}" "${bench_file}"

# Binlog replication from this shouldn't bleed over into the next test.
printf "[SLEEP] Waiting %s for binlog replication to settle in." "${REPL_AWAIT_SLEEP_DURATION}"
sleep "${REPL_AWAIT_SLEEP_DURATION}"

# TODO @Marcus: Remove short circuiting once this error is mitigated:
# Query name exists but existing query is different: q
exit 0

# --------------------------------------------
# Fastly Small Cache Hit Benchmark
# --------------------------------------------
bench_file='cache_hit_fastly_small.yaml'
project='Fastly'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}" "1"

benchmarks --benchmark "$BENCHMARK_FILE_DIR/$bench_file"

check_benchmark_exit_code $? "${project}" "${bench_file}"

# --------------------------------------------
# Fastly Small Read Benchmark
# --------------------------------------------
bench_file='read_benchmark_fastly_small.yaml'
project='Fastly'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}" "1"

benchmarks \
    --run-for "${FASTLY_READ_BENCHMARK_DURATION}" \
    --skip-setup \
    --benchmark "$BENCHMARK_FILE_DIR/$bench_file"

check_benchmark_exit_code $? "${project}" "${bench_file}"

# --------------------------------------------
# Small Connection Scaling Benchmark
# --------------------------------------------
bench_file="scale_connections_small.yaml"
project='Internal'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}"  "1"

benchmarks --benchmark "$BENCHMARK_FILE_DIR/$bench_file"

check_benchmark_exit_code $? "${project}" "${bench_file}"

# This benchmark creates an incredible number of views and doesn't clean them up.
# It should be run last.
bench_file='scale_views_small.yaml'
project='Internal'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}" "1"

benchmarks --benchmark "${BENCHMARK_FILE_DIR}/$bench_file"

check_benchmark_exit_code $? "${project}" "${bench_file}"

# ----------- [Complete] ---------------------------------------- #

end_time=$(date +%s)
runtime=$((end_time-BENCHMARK_START_TIME))
printf "All benchmarks completed successfully in a total of %s seconds." $runtime
