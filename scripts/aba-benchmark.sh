#!/bin/bash
set -eo pipefail

DIR="$(cd "$(dirname "${0}")/.."; pwd)"
cd "${DIR}"

function ensure_monitoring_stack() {
    docker-compose -f "${DIR}/docker/monitoring/docker-compose.yml" up -d
}

function background() {
    $@ &>/dev/null &
    echo $!
}

function run_noria() {
    background "${DIR}/target/release/noria-server" --deployment localbench --prometheus-metrics --noria-metrics --replication-url=mysql://root:noria@localhost:3306/noria
}

function run_adapter() {
    background "${DIR}/target/release/noria-mysql" --deployment localbench --prometheus-metrics --upstream-db-url=mysql://root:noria@localhost:3306/noria --allow-unauthenticated-connections --address 0.0.0.0:3307
}

function setup() {
    docker-compose -f "${DIR}/docker-compose.yml" -f "${DIR}/benchmarks/docker-compose.override.yml" up -d
    cargo build --release --bin noria-server --bin noria-mysql --bin benchmarks & sleep 10; wait
    exec 3< <(run_noria)
    exec 4< <(run_adapter)
    read <&3 noria_pid
    read <&4 adapter_pid
    echo "$noria_pid $adapter_pid"
    until mysqladmin -h127.0.0.1 -P3307 ping &>/dev/null; do
        sleep 1
    done
}

function run_benchmarks() {
    export RDS_HOST=127.0.0.1
    export RDS_PORT=3306
    export RDS_USER=root
    export RDS_PASS=noria
    export RDS_DATABASE=noria
    export READYSET_ADAPTER=mysql://noria@localhost:3307/noria
    export PROMETHEUS_ADDRESS=http://localhost:9091
    export INSTANCE_LABEL="${1//\//-}"
    "${DIR}/scripts/run-benchmark.sh"
}

function teardown() {
    kill "$1" "$2"
    docker-compose -f "${DIR}/docker-compose.yml" -f "${DIR}/benchmarks/docker-compose.override.yml" down -v
    rm -Rf localbench-*
}

if [ "$#" -ne 2 ]; then
    echo "usage: ${0} a_committish b_committish"
    echo
    echo 'Performs A-B-A benchmark comparison using the following workflow:'
    echo '  1. Ensure monitoring stack is up (Prometheus, etc)'
    echo '  2. Checkout a_committish'
    echo '  3. Build the codebase and start a Noria cluster'
    echo '  4. Run benchmarks, tagging metrics with a_committish'
    echo '  5. Checkout b_committish'
    echo '  6. Build the codebase and start a Noria cluster'
    echo '  7. Run benchmarks, tagging metrics with b_committish'
    echo '  8. Checkout a_committish'
    echo '  9. Build the codebase and start a Noria cluster'
    echo ' 10. Run benchmarks, tagging metrics with a_committish'
    echo ' 11. Direct you to use your human eyes and brain to view and interpret metrics'
    exit 1
fi

ensure_monitoring_stack

echo '--- A1'
git checkout "${1}"
pids="$(setup)"
run_benchmarks "${1}-1"
teardown $pids

echo '--- B'
git checkout "${2}"
pids="$(setup)"
run_benchmarks "${2}"
teardown $pids

echo '--- A2'
git checkout "${1}"
pids="$(setup)"
run_benchmarks "${1}-2"
teardown $pids

echo
echo 'Benchmarks complete.  You can now load up Grafana on http://localhost:3000/ and explore metrics.'
