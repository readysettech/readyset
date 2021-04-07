#!/usr/bin/env bash
set -euo pipefail

sudo tee /etc/default/noria-mysql > /dev/null <<EOF
NORIA_DEPLOYMENT=${deployment}
ZOOKEEPER_URL=${zookeeper_ip}:2181
MYSQL_URL=${mysql_connection_string}
EOF

systemctl reset-failed
systemctl enable noria-mysql
systemctl restart noria-mysql
