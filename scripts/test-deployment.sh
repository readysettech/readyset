#!/bin/bash
set -euf -o pipefail
# ./test-deployment.sh <data directory>.
#  data-directory: The directory with the files packaged by
#                  package-deployment-tests.sh
#
# This runs the complete deployment test suite:
#  1. Run the basic validation test that creates simple tables and queries.
#  2. Run irl-minimal which tests that every irl query can run.
#  3. Run a high throuhgput read-benchmark.
#  4. Verify that our performance is reasonable recorded in prometheus.
#
# ENV variables:
#   RDS_HOST: The hostname of the RDS instance.
#   RDS_PORT: The port of the RDS instance.
#   RDS_USER: The username of the RDS database.
#   RDS_PASS: The password used for the RDS database.
#   RDS_DATABASE: The database noria is caching in RDS.
#   READYSET_ADAPTER: The adapter's NLB endpoint.
#                     i.e. mysql://<username>:<password>@<addr>:<port>
#   PROMETHEUS_ADDRESS: The address of the monitor's prometheus server.
#                        i.e. 127.0.0.1:9091 on the monitor instance.

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
# Small read benchmark definition.
BENCHMARK_FILE=read_benchmark_irl_small.yaml
# Schema of the IRL database to use for benchmarks.
IRL_SCHEMA=src/data/irl/irl_db_small.sql

# Write the deployment YAML for use by benchmarks later.
cat > $DEPLOYMENT_FILE <<EOF
---
instance_label: local
prometheus_push_gateway: ~
target_conn_str: "$READYSET_ADAPTER"
setup_conn_str: "$READYSET_RDS"
EOF

echo "Beginning basic query validation test"
./basic_validation_test --adapter $READYSET_ADAPTER --rds $READYSET_RDS --prometheus-address $PROMETHEUS_ADDRESS --migration-mode explicit-migration

echo "Installing schema: $IRL_SCHEMA"
mysql -h $RDS_HOST --port $RDS_PORT -u $RDS_USER -p$RDS_PASS $RDS_DATABASE < $IRL_SCHEMA

echo "Generating data for $IRL_SCHEMA".
./data_generator --schema $IRL_SCHEMA --database-url $READYSET_RDS

echo "Sleeping for 2 minutes so ReadySet has time to perform migrations and propagate data."
sleep 2m

echo "Testing IRL demo queries"
# TODO (justin): Add explicit migrations flag when merged.
./irl_minimal --mysql-url $READYSET_ADAPTER --explicit-migrations

echo "Beginning benchmark!"
./benchmarks --skip-setup --benchmark $BENCHMARK_FILE --deployment $DEPLOYMENT_FILE

echo "Verifying end-to-end latency numbers are < 20ms at 90p"
./verify_prometheus_metrics end-to-end-latency
