#!/usr/bin/env bash
set -euo pipefail

IMDS_TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
SERVER_ADDRESS=$(curl -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

OPT_PATH="/opt/readyset/vector.d"
CONFIG_DIR="/etc/vector"
BASE="base.toml"
METADATA_CW="metadata_to_cw.toml"
METRICS_CW="metrics_to_cw.toml"
METADATA_AGG="metadata_to_agg.toml"
METRICS_AGG="metrics_to_agg.toml"
LOOKUP_AGGREGATOR=false

MAX_RETRIES=8
failures=0
LOG_AGGREGATOR_ADDRESS=""

cp ${OPT_PATH}/${BASE} ${CONFIG_DIR}/${BASE}

if [ ${ENABLE_CW} = true ]; then
  cp ${OPT_PATH}/${METADATA_CW} ${CONFIG_DIR}/${METADATA_CW}
  cp ${OPT_PATH}/${METRICS_CW} ${CONFIG_DIR}/${METRICS_CW}
fi

if [ ${ENABLE_AGG} = true ]; then
  cp ${OPT_PATH}/${METADATA_AGG} ${CONFIG_DIR}/${METADATA_AGG}
  cp ${OPT_PATH}/${METRICS_AGG} ${CONFIG_DIR}/${METRICS_AGG}
  LOOKUP_AGGREGATOR=true
fi

while [ ${LOOKUP_AGGREGATOR} = true ] && [ -z ${LOG_AGGREGATOR_ADDRESS} ] && [ ${failures} -lt ${MAX_RETRIES} ]; do
  if [ $failures -gt 1 ]; then
    sleep $((2 ** failures))
  fi
  LOG_AGGREGATOR_ADDRESS=$(dig @127.0.0.1 -p 8600 logs-aggregator.service.consul +short)
  failures=$((failures + 1))
done

cat > /etc/default/vector <<EOF
AWS_CLOUDFORMATION_STACK=${AWS_CLOUDFORMATION_STACK}
AWS_CLOUDFORMATION_REGION=${AWS_CLOUDFORMATION_REGION}
PROMETHEUS_PORT=${PROMETHEUS_PORT}
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_TYPE=${NORIA_TYPE}
SERVER_ADDRESS=${SERVER_ADDRESS}
LOG_AGGREGATOR_ADDRESS=${LOG_AGGREGATOR_ADDRESS}
VECTOR_CONFIG_DIR=${CONFIG_DIR}
EOF

systemctl reset-failed
systemctl enable vector
systemctl restart vector
