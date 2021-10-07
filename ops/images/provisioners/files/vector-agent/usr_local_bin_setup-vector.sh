#!/usr/bin/env bash
set -euo pipefail

LOG_AGGREGATOR_ADDRESS=`dig @127.0.0.1 -p 8600 logs-aggregator.service.consul +short`

if [ -z "${LOG_AGGREGATOR_ADDRESS}" ]
then
  exit 1
fi

cat > /etc/vector.d/vector.toml <<EOF
[sources.in]
type = "journald"
include_units = [ "readyset-server", "readyset-mysql-adapter" ]

[transforms.metadata]
type = "remap"
inputs = ["in"]
source = '''
  log_json = parse_json!(.message)
  server_info = { "host": .host, "app": .SYSLOG_IDENTIFIER }
  . = merge!(log_json, server_info)
'''

[sinks.out]
inputs = ["metadata"]
type = "vector"
address = "${LOG_AGGREGATOR_ADDRESS}:9000"
EOF
