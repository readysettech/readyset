#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

on_error() {
  local exit_code="$?"

  /usr/local/bin/cfn-signal-wrapper.sh "$exit_code"
}

trap 'on_error' ERR

/usr/local/bin/cfn-init-wrapper.sh
/usr/local/bin/set-host-description.sh
/usr/local/bin/configure-consul-client.sh
# Build connection string from inputs
source /usr/local/bin/get-connection-string.sh
REPLICATION_URL=${DB_URL}

export NORIA_TYPE="readyset-server"
export PROMETHEUS_PORT=6033
/usr/local/bin/configure-vector-agent.sh

NORIA_MEMORY_BYTES=$((${NORIA_MEMORY_LIMIT_GB}<<30))

cat > /etc/default/readyset-server <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_MEMORY_BYTES=${NORIA_MEMORY_BYTES}
NORIA_PRIMARY_REGION=${NORIA_PRIMARY_REGION}
NORIA_QUORUM=${NORIA_QUORUM}
NORIA_REGION=${NORIA_REGION}
NORIA_SHARDS=${NORIA_SHARDS}
REPLICATION_URL=${REPLICATION_URL}
AUTHORITY_ADDRESS=${AUTHORITY_ADDRESS:-127.0.0.1:8500}
LOG_LEVEL=${LOG_LEVEL:-info}
EOF
chmod 600 /etc/default/readyset-server

cat > /etc/default/ensure-ebs-volume <<EOF
AWS_CLOUDFORMATION_STACK=${AWS_CLOUDFORMATION_STACK}
AWS_CLOUDFORMATION_RESOURCE=${AWS_CLOUDFORMATION_RESOURCE}
AWS_CLOUDFORMATION_REGION=${AWS_CLOUDFORMATION_REGION}
SQS_QUEUE_URL=${SQS_QUEUE_URL}
NORIA_QUORUM=${NORIA_QUORUM}
INITIAL_VOLUME_SIZE_GB=${INITIAL_VOLUME_SIZE_GB}
VOLUME_TAG_KEY="ReadySet:ServerVolume"
VOLUME_TAG_VALUE=${DEPLOYMENT}
INSTANCE_TAG_KEY="ReadySet:ServerInstance"
INSTANCE_TAG_VALUE=${DEPLOYMENT}
EOF

if [ "$AUTO_GROW_VOLUME" = "true" ]; then
  echo "AUTO_GROW_VOLUME=true" >> /etc/default/ensure-ebs-volume

  if [ "$MAX_VOLUME_SIZE_GB" != "0" ]; then
    echo "MAX_VOLUME_SIZE_GB=${MAX_VOLUME_SIZE_GB}" >> /etc/default/ensure-ebs-volume
  fi
fi


mkdir -p /var/lib/readyset-server
systemctl reset-failed
systemctl enable ensure-ebs-volume
systemctl start ensure-ebs-volume

/usr/local/bin/cfn-signal-wrapper.sh 0

# health check will not run for 30 mins
touch /tmp/.lasthealthcheck -d '-20 minute' # store timstamp for last healthcheck
