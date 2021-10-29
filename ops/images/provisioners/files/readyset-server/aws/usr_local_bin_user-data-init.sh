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
/usr/local/bin/configure-consul-client.sh
/usr/local/bin/configure-vector.sh

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
EOF
chmod 600 /etc/default/readyset-server

IMDS_TOKEN=`curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"`
SERVER_ADDRESS=`curl -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4`

cat >> /etc/vector.d/env <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_TYPE="readyset-server"
SERVER_ADDRESS=${SERVER_ADDRESS}
EOF

cat > /etc/default/ensure-ebs-volume <<EOF
AWS_CLOUDFORMATION_STACK=${AWS_CLOUDFORMATION_STACK}
AWS_CLOUDFORMATION_RESOURCE=${AWS_CLOUDFORMATION_RESOURCE}
AWS_CLOUDFORMATION_REGION=${AWS_CLOUDFORMATION_REGION}
SQS_QUEUE_URL=${SQS_QUEUE_URL}
VOLUME_SIZE_GB=${VOLUME_SIZE_GB}
VOLUME_TAG_KEY="ReadySet:ServerVolume"
VOLUME_TAG_VALUE=${DEPLOYMENT}
INSTANCE_TAG_KEY="ReadySet:ServerInstance"
INSTANCE_TAG_VALUE=${DEPLOYMENT}
EOF

mkdir -p /var/lib/readyset-server
systemctl reset-failed
systemctl enable ensure-ebs-volume
systemctl start ensure-ebs-volume

/usr/local/bin/cfn-signal-wrapper.sh 0
