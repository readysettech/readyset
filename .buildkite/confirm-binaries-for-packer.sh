#!/bin/sh
# Makes sure there are files in the right locations for packer to make builds
# from for validation and test builds.
set -eux

project_root=$(realpath "$(dirname $0)/..")
target_debug="$project_root/target/debug"
mkdir -p "$target_debug"
touch "$target_debug/ensure-ebs-volume"
touch "$target_debug/noria-mysql"
touch "$target_debug/noria-psql"
touch "$target_debug/noria-server"
touch "$target_debug/metrics-aggregator"
touch "$target_debug/basic_validation_test"
touch "$target_debug/telemetry-ingress"
