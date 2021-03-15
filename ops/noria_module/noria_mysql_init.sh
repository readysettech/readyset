#!/usr/bin/env bash
set -euo pipefail

systemctl stop noria-mysql

sudo tee /etc/default/noria-mysql > /dev/null <<EOF
NORIA_DEPLOYMENT=${deployment}
ZOOKEEPER_URL=${zookeeper_ip}:2181
EOF

systemctl reset-failed
systemctl restart noria-mysql
