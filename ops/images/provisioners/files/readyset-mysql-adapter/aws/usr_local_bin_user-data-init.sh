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

cat > /etc/default/readyset-mysql-adapter <<EOF
UPSTREAM_DB_URL=${UPSTREAM_DB_URL}
NORIA_DEPLOYMENT=${DEPLOYMENT}
AUTHORITY_ADDRESS=${AUTHORITY_ADDRESS:-127.0.0.1:8500}
EOF
chmod 600 /etc/default/readyset-mysql-adapter

systemctl reset-failed
systemctl enable readyset-mysql-adapter
systemctl restart readyset-mysql-adapter

/usr/local/bin/cfn-signal-wrapper.sh 0
