#!/usr/bin/env bash
set -euo pipefail

systemctl stop debezium-connector

sudo tee /etc/default/debezium-connector > /dev/null <<EOF
TABLES=${tables}
SERVER_NAME=${db_name}
DB_NAME=${db_name}
ZOOKEEPER_URL=${zookeeper_ip}:2181
KAFKA_URL=${kafka_ip}:9092
NORIA_DEPLOYMENT=${deployment}
EOF

systemctl reset-failed
systemctl restart debezium-connector
