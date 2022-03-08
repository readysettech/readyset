#!/bin/bash
set -euf -x -o pipefail
shopt -s nullglob # have globs expand to nothing when they don't match

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
BENCHMARK_TEST_DIR=${BENCHMARK_FILE_DIR:-/usr/src/app/src/yaml/benchmarks}
BENCHMARK_EAGER_EXIT="${BENCHMARK_EAGER_EXIT:-'enabled'}"
REPL_AWAIT_SLEEP_DURATION=${REPL_AWAIT_SLEEP_DURATION:-120}

# Used to calculate total duration
BENCHMARK_START_TIME=$(date +%s)

# Location to pass to benchmark binary as --results-file
REPORT_SAVE_DIR=${REPORT_SAVE_DIR:-/tmp/artifacts}
REPORT_JSON_ENABLED="${REPORT_JSON_ENABLED:-'disabled'}"

# ----------- [Functions] ----------------------------------- #

log_benchmark () {
    project=$1
    bench_f=$2
    msg=$3
    printf "[%s - %s] %s\n" "${project}" "${bench_f}" "${msg}"
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

append_json_output_file_b64() {
    local file_name=$1
    local file_path=$2
    local b64
    local json
    if [[ "${REPORT_JSON_ENABLED}" != "enabled" ]]; then
        return
    fi
    b64=$(base64 -i $file_path | tr -d '\n')
    json='{"'"$file_name"'":"'"${b64}"'"}'
    if  [[ -n "${b64}" ]]; then
        results_json_output=$(echo $results_json_output | jq '. + '"$json"'')
    fi
}

print_results_json() {
    if [[ "${REPORT_JSON_ENABLED}" != "enabled" ]]; then
        return
    fi
    set +x
    echo "BENCH_RUNNER_OUTPUT${results_json_output}END"
    set -x
}

# ----------- [Main] ---------------------------------------- #

results_json_output="{}"
echo 'Beginning execution of ReadySet MySQL benchmarks'

# --------------------------------------------
# IRL Small Read Benchmark
# --------------------------------------------
bench_subdir='test'
bench_file='read_benchmark_irl_small.yaml'
project='irl'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}" "1"

benchmarks \
    --skip-setup \
    --benchmark "$BENCHMARK_TEST_DIR/$bench_subdir/$bench_file" \
    --results-file "${REPORT_SAVE_DIR}/$project.log"
ec=$?

append_json_output_file_b64 "${project}.log" "${REPORT_SAVE_DIR}/$project.log"
check_benchmark_exit_code $ec "${project}" "${bench_file}"

# Binlog replication from this shouldn't bleed over into the next test.
printf "[SLEEP] Waiting %s for binlog replication to settle in." "${REPL_AWAIT_SLEEP_DURATION}"
sleep "${REPL_AWAIT_SLEEP_DURATION}"

# --------------------------------------------
# Minimal Benchmarks
# --------------------------------------------
echo "Dynamically identifying minimal benchmarks to be executed."
bench_subdir='minimal'
project='minimal'
benchmark_files=`find $BENCHMARK_TEST_DIR/minimal -mindepth 1`
setup_str=''

echo "Preparing to execute Minimal benchmarks."

for bench_path in $benchmark_files; do
    bench_name=${bench_subdir}/$(basename $bench_path)
    echo "Bench Name: $bench_name"
    echo "Bench Path: $bench_path"
    log_benchmark "${project}" "${bench_name}" "${BENCH_LOG_START_MSG}" "1"

    benchmarks \
        --benchmark $bench_path \
        ${setup_str} \
        --results-file "${REPORT_SAVE_DIR}/$project.log"
    ec=$?
    # Skip on every minimal benchmark after the first.
    setup_str='--skip-setup'
    append_json_output_file_b64 "${project}.log" "${REPORT_SAVE_DIR}/$project.log"
    check_benchmark_exit_code $ec "${project}" "${bench_name}"
done

# --------------------------------------------
# Fastly Small Cache Hit Benchmark
# --------------------------------------------
bench_subdir='test'
bench_file='cache_hit_fastly_small.yaml'
project='fastly'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}" "1"

benchmarks \
    --benchmark "$BENCHMARK_TEST_DIR/$bench_subdir/$bench_file" \
    --results-file "${REPORT_SAVE_DIR}/$project.log"
ec=$?

append_json_output_file_b64 "${project}.log" "${REPORT_SAVE_DIR}/$project.log"
check_benchmark_exit_code $ec "${project}" "${bench_file}"

# --------------------------------------------
# Fastly Small Read Benchmark
# --------------------------------------------
bench_subdir='test'
bench_file='read_benchmark_fastly_small.yaml'
project='fastly'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}" "1"

benchmarks \
    --skip-setup \
    --benchmark "$BENCHMARK_TEST_DIR/$bench_subdir/$bench_file" \
    --results-file "${REPORT_SAVE_DIR}/$project.log"
ec=$?

append_json_output_file_b64 "${project}.log" "${REPORT_SAVE_DIR}/$project.log"
check_benchmark_exit_code $ec "${project}" "${bench_file}"

# --------------------------------------------
# Small Connection Scaling Benchmark
# --------------------------------------------
bench_subdir='test'
bench_file="scale_connections_small.yaml"
project='internal'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}"  "1"

benchmarks \
    --benchmark "$BENCHMARK_TEST_DIR/$bench_subdir/$bench_file" \
    --results-file "${REPORT_SAVE_DIR}/$project.log"

check_benchmark_exit_code $? "${project}" "${bench_file}"

# This benchmark creates an incredible number of views and doesn't clean them up.
# It should be run last.
bench_subdir='test'
bench_file='scale_views_small.yaml'
project='internal'
log_benchmark "${project}" "${bench_file}" "${BENCH_LOG_START_MSG}" "1"

benchmarks \
    --benchmark "$BENCHMARK_TEST_DIR/$bench_subdir/$bench_file" \
    --results-file "${REPORT_SAVE_DIR}/$project.log"
ec=$?

append_json_output_file_b64 "${project}.log" "${REPORT_SAVE_DIR}/$project.log"
check_benchmark_exit_code $ec "${project}" "${bench_file}"

# ----------- [Complete] ---------------------------------------- #

end_time=$(date +%s)
runtime=$((end_time-BENCHMARK_START_TIME))
printf "All benchmarks completed successfully in a total of %s seconds." $runtime

print_results_json
