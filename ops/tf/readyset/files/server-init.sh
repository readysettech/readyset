#!/bin/sh

${SETUP_DATA_VOLUME}

cat > /etc/default/readyset-server <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_MEMORY_BYTES=${MEMORY_BYTES}
NORIA_PRIMARY_REGION=${PRIMARY_REGION}
NORIA_QUORUM=${QUORUM}
NORIA_REGION=${REGION}
NORIA_SHARDS=${SHARDS}
REPLICATION_URL=${MYSQL_URL}
ZOOKEEPER_URL=${ZOOKEEPER_URL}
EOF
chmod 600 /etc/default/readyset-server

systemctl stop readyset-server
setup_data_volume /var/lib/readyset-server
systemctl reset-failed
systemctl start readyset-server
