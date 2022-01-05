#!/bin/bash
set -euf -x -o pipefail
# ./run-benchmarks.sh <data directory with all benchmarks / sql files>
#
# ENV variables:
#   RDS_HOST: The hostname of the RDS instance.
#   RDS_PORT: The port of the RDS instance.
#   RDS_USER: The username of the RDS database.
#   RDS_PASS: The password used for the RDS database.
#   READYSET_ADAPTER: The adapter's NLB endpoint.
#   PROMETHEUS_ADDRESS: The address of the monitor's prometheus server.

DATA_DIR="./"
if [ "$#" -eq 1 ]; then
    DATA_DIR=$1
fi

# Go to the directory that has all test scripts.
cd $DATA_DIR

# MySQL connection string for the RDS.
READYSET_RDS=mysql://$RDS_USER:$RDS_PASS@$RDS_HOST:$RDS_PORT/$RDS_DATABASE
# File to write deployment yaml files to.
DEPLOYMENT_FILE=deployment.yaml

# Write the deployment YAML for use by benchmarks later.
cat > $DEPLOYMENT_FILE <<EOF
---
instance_label: local
prometheus_push_gateway: ~
target_conn_str: "$READYSET_ADAPTER"
setup_conn_str: "$READYSET_RDS"
EOF

echo "Beginning benchmarks"

./benchmarks --benchmark read_benchmark_irl_small.yaml --deployment $DEPLOYMENT_FILE

# Binlog replication from this shouldn't bleed over into the next test.
sleep 120

./benchmarks --benchmark cache_hit_fastly_small.yaml --deployment $DEPLOYMENT_FILE

./benchmarks --benchmark read_benchmark_fastly_small.yaml --deployment $DEPLOYMENT_FILE

./benchmarks --benchmark scale_connections_small.yaml --deployment $DEPLOYMENT_FILE

# This benchmark creates an incredibly number of views and doesn't clean them up.
# It should be run last.
./benchmarks --benchmark scale_views_small.yaml --deployment $DEPLOYMENT_FILE
