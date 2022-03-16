#!/usr/bin/env bash
set -euo pipefail

IMDS_TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
SERVER_ADDRESS=$(curl -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

MAX_RETRIES=8
failures=0
LOG_AGGREGATOR_ADDRESS=""
while [ -z "${LOG_AGGREGATOR_ADDRESS}" ] && [ $failures -lt $MAX_RETRIES ]; do
  if [ $failures -gt 1 ]; then
    sleep $((2 ** failures))
  fi
  LOG_AGGREGATOR_ADDRESS=$(dig @127.0.0.1 -p 8600 logs-aggregator.service.consul +short)
  failures=$((failures + 1))
done

cat > /etc/default/vector <<EOF
PROMETHEUS_PORT=${PROMETHEUS_PORT}
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_TYPE=${NORIA_TYPE}
SERVER_ADDRESS=${SERVER_ADDRESS}
LOG_AGGREGATOR_ADDRESS=${LOG_AGGREGATOR_ADDRESS}
EOF

systemctl reset-failed
systemctl enable vector
systemctl restart vector
