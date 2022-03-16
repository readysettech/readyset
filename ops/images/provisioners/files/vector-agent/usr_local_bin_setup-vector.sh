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

[sources.node-exporter]
type = "prometheus_scrape"
endpoints = [ "http://localhost:9100/metrics" ]
scrape_interval_secs = 15

[sources.prometheus]
type = "prometheus_scrape"
endpoints = [ "http://localhost:${PROMETHEUS_PORT:-6033}/prometheus" ]
scrape_interval_secs = 15

[transforms.metrics]
type = "remap"
inputs = ["node-exporter", "prometheus"]
source = '''
  .tags.deployment = "${NORIA_DEPLOYMENT}"
  .tags.job = "${NORIA_TYPE}"
  .tags.instance = "${SERVER_ADDRESS}"
'''

[transforms.metadata]
type = "remap"
inputs = ["in"]
source = '''
  server_info = { "host": .host, "app": .SYSLOG_IDENTIFIER }
  structured, err = parse_json(.message)
  . = merge(., server_info)
  if err == null {
    . = merge!(., structured)
  }
'''

[sinks.out]
inputs = ["metadata", "metrics"]
type = "vector"
address = "${LOG_AGGREGATOR_ADDRESS}:9000"
EOF
