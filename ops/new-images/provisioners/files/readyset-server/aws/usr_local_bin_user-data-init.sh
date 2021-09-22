#!/bin/bash
set -eux -o pipefail

on_error() {
  local exit_code="$?"

  /usr/local/bin/cfn-signal-wrapper.sh "$exit_code"
}

trap 'on_error' ERR

/usr/local/bin/cfn-init-wrapper.sh

cat > /etc/default/readyset-server <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_MEMORY_BYTES=${MEMORY_BYTES}
NORIA_PRIMARY_REGION=${PRIMARY_REGION}
NORIA_QUORUM=${QUORUM}
NORIA_REGION=${REGION}
NORIA_SHARDS=${SHARDS}
REPLICATION_URL=${MYSQL_URL}
ZOOKEEPER_ADDRESS=${ZOOKEEPER_ADDRESS}
EOF
chmod 600 /etc/default/readyset-server

setup_data_volume /var/lib/readyset-server

systemctl reset-failed
systemctl enable readyset-server
systemctl restart readyset-server

/usr/local/bin/cfn-signal-wrapper.sh 0
