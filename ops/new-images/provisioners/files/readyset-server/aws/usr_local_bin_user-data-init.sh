#!/bin/bash
set -eux -o pipefail

on_error() {
  local exit_code="$?"

  /usr/local/bin/cfn-signal-wrapper.sh "$exit_code"
}

trap 'on_error' ERR

/usr/local/bin/cfn-init-wrapper.sh

setup-data-volume /var/lib/readyset-server

if [ -f /var/lib/readyset-server/volume_id ]; then
  volume_id=$(< /var/lib/readyset-server/volume_id)
else
  volume_id=$(< /proc/sys/kernel/random/uuid)
  echo -n "$volume_id" > /var/lib/readyset-server/volume_id
fi

cat > /etc/default/readyset-server <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_MEMORY_BYTES=${MEMORY_BYTES}
NORIA_PRIMARY_REGION=${PRIMARY_REGION}
NORIA_QUORUM=${QUORUM}
NORIA_REGION=${REGION}
NORIA_SHARDS=${SHARDS}
REPLICATION_URL=${MYSQL_URL}
ZOOKEEPER_ADDRESS=${ZOOKEEPER_ADDRESS}
VOLUME_ID=${volume_id}
EOF
chmod 600 /etc/default/readyset-server

systemctl reset-failed
systemctl enable readyset-server
systemctl restart readyset-server

/usr/local/bin/cfn-signal-wrapper.sh 0
