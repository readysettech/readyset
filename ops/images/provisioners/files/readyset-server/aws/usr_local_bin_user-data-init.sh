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

mkdir -p /var/lib/readyset-server
ensure-ebs-volume /var/lib/readyset-server --volume-size-gb "${VOLUME_SIZE_GB:-32}"

if [ -f /var/lib/readyset-server/volume_id ]; then
  volume_id=$(< /var/lib/readyset-server/volume_id)
else
  volume_id=$(< /proc/sys/kernel/random/uuid)
  echo -n "$volume_id" > /var/lib/readyset-server/volume_id
fi

cat > /etc/default/readyset-server <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_MEMORY_BYTES=${NORIA_MEMORY_BYTES}
NORIA_PRIMARY_REGION=${NORIA_PRIMARY_REGION}
NORIA_QUORUM=${NORIA_QUORUM}
NORIA_REGION=${NORIA_REGION}
NORIA_SHARDS=${NORIA_SHARDS}
REPLICATION_URL=${REPLICATION_URL}
AUTHORITY_ADDRESS=${AUTHORITY_ADDRESS:-127.0.0.1:8500}
VOLUME_ID=${volume_id}
EOF
chmod 600 /etc/default/readyset-server

systemctl reset-failed
systemctl enable readyset-server
systemctl restart readyset-server

/usr/local/bin/cfn-signal-wrapper.sh 0
