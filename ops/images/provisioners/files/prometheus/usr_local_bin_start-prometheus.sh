#!/usr/bin/env bash
set -euo pipefail

log_aggregator_address=`dig @127.0.0.1 -p 8600 logs-aggregator.service.consul +short`

if [ -z "${log_aggregator_address}" ]
then
  exit 1
fi

cat > /etc/prometheus.d/prometheus.yaml <<EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["${log_aggregator_address}:9090"]
    honor_labels: true
EOF

/usr/local/bin/prometheus \
  --config.file /etc/prometheus.d/prometheus.yaml \
  --storage.tsdb.path /var/lib/prometheus \
  --web.listen-address="0.0.0.0:9091"
