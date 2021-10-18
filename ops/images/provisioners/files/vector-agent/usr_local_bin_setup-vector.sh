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
include_units = [ "readyset-mysql-adapter", "readyset-server" ]

[sources.prometheus]
type = "prometheus_scrape"
endpoints = [ "http://localhost:${PROMETHEUS_PORT:-6033}/prometheus" ]
scrape_interval_secs = 15

[transforms.metadata]
type = "remap"
inputs = ["in"]
source = '''
  log_json = parse_json!(.message)
  server_info = { "host": .host, "app": .SYSLOG_IDENTIFIER }
  . = merge!(log_json, server_info)
'''

[sinks.out]
inputs = ["metadata", "prometheus"]
type = "vector"
address = "${LOG_AGGREGATOR_ADDRESS}:9000"
EOF
