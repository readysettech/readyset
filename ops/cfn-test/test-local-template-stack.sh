#!/usr/bin/env bash
set -euo pipefail
key_pair_name=$1

# Upload templates to personal s3 bucket
cd ../cfn || exit
taskcat upload

# start a new test stack
cd ../cfn-test || exit
bash create-mysql-super-stack.sh "$key_pair_name"
