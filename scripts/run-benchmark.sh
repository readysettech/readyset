#!/bin/bash
set -euf -x -o pipefail
# ./run-benchmarks.sh <data directory with all benchmarks / sql files>
#
# ENV variables:
#   RDS_HOST: The hostname of the RDS instance.
#   RDS_PORT: The port of the RDS instance.
#   RDS_USER: The username of the RDS database.
#   RDS_PASS: The password used for the RDS database.
#   RDS_DATABASE: The database name used for RDS.
#   READYSET_ADAPTER: The adapter's NLB endpoint.
#   PROMETHEUS_ADDRESS: The address of the monitor's prometheus server.
#   INSTANCE_LABEL: Value to apply to "instance" label on Prometheus metrics;
#                   defaults to "local".

ROOT="$(cd "$(dirname "${0}")/.."; pwd)"
DATA_DIR="${ROOT}/benchmarks/src/yaml/benchmarks/test"
if [ "$#" -eq 1 ]; then
    DATA_DIR="$1"
fi

# Go to the directory that has all test scripts.
cd "${ROOT}/benchmarks"

# MySQL connection string for the RDS.
READYSET_RDS=mysql://$RDS_USER:$RDS_PASS@$RDS_HOST:$RDS_PORT/$RDS_DATABASE
# File to write deployment yaml files to.
DEPLOYMENT_FILE=deployment.yaml

# Write the deployment YAML for use by benchmarks later.
cat > $DEPLOYMENT_FILE <<EOF
---
instance_label: "${INSTANCE_LABEL:-local}"
prometheus_push_gateway: "${PROMETHEUS_ADDRESS:-~}"
target_conn_str: "$READYSET_ADAPTER"
setup_conn_str: "$READYSET_RDS"
EOF

echo "Beginning benchmarks"

cargo run --release --bin benchmarks -- --benchmark "${DATA_DIR}/read_benchmark_irl_small.yaml" --deployment $DEPLOYMENT_FILE

# Binlog replication from this shouldn't bleed over into the next test.
sleep 120

cargo run --release --bin benchmarks -- --benchmark "${DATA_DIR}/cache_hit_fastly_small.yaml" --deployment $DEPLOYMENT_FILE

cargo run --release --bin benchmarks -- --benchmark "${DATA_DIR}/read_benchmark_fastly_small.yaml" --deployment $DEPLOYMENT_FILE --skip-setup

cargo run --release --bin benchmarks -- --benchmark "${DATA_DIR}/scale_connections_small.yaml" --deployment $DEPLOYMENT_FILE

# This benchmark creates an incredibly number of views and doesn't clean them up.
# It should be run last.
cargo run --release --bin benchmarks -- --benchmark "${DATA_DIR}/scale_views_small.yaml" --deployment $DEPLOYMENT_FILE
