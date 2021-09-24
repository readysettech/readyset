#!/bin/bash
set -eux -o pipefail

on_error() {
  local exit_code="$?"

  /usr/local/bin/cfn-signal-wrapper.sh "$exit_code"
}

trap 'on_error' ERR

/usr/local/bin/cfn-init-wrapper.sh

/usr/local/bin/configure-consul-client.sh

cat > /etc/default/readyset-mysql-adapter <<EOF
MYSQL_URL=${MYSQL_URL}
NORIA_DEPLOYMENT=${DEPLOYMENT}
AUTHORITY_ADDRESS=${AUTHORITY_ADDRESS:-127.0.0.1:8500}
EOF
chmod 600 /etc/default/readyset-mysql-adapter

systemctl reset-failed
systemctl enable readyset-mysql-adapter
systemctl restart readyset-mysql-adapter

/usr/local/bin/cfn-signal-wrapper.sh 0
