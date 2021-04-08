#!/usr/bin/env bash
set -euo pipefail

systemctl stop debezium-connect

# Patch the systemd unit until we do it in Packer image
# Ideally we should use something like ConditionPathExists for checking if /etc/default/debezium-connect exists
sed -i '/^ExecStartPre/d' /etc/systemd/system/debezium-connect.service
sed -i '/^ExecStart=.*/i ExecStartPre=\/usr\/bin\/docker system prune -a -f --volumes' /etc/systemd/system/debezium-connect.service
systemctl daemon-reload

sudo tee /etc/default/debezium-connect > /dev/null <<EOF
BOOTSTRAP_SERVERS=${kafka_ip}:9092
EOF

systemctl reset-failed
systemctl restart debezium-connect

# Wait 120 seconds for it to come up
set +e
timeout 120 bash <<EOF
  until echo 2>>/dev/null >>/dev/tcp/localhost/8083
    do sleep 1
  done
EOF
if [ "$?" = 124 ]; then
    >&2 echo 'Debezium connect did not starting connections after 120 seconds'
    exit 1
fi
set -e

curl -i -X POST \
    -H "Accept:application/json" \
    -H "Content-Type:application/json" \
    localhost:8083/connectors \
    --data-binary @- <<EOF
{
  "name": "noria-${db_name}-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "${db_host}",
    "database.port": "3306",
    "database.user": "${db_user}",
    "database.password": "${db_password}",
    "database.server.id": "184054",
    "database.server.name": "${db_name}",
    "database.include.list": "${db_name}",
    "database.history.kafka.bootstrap.servers": "${kafka_ip}:9092",
    "database.history.kafka.topic": "dbhistory.${db_name}",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "decimal.handling.mode": "double",
    "time.precision.mode": "connect"
  }
}
EOF
