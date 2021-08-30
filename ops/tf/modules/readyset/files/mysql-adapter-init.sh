#!/bin/sh

cat > /etc/default/readyset-mysql-adapter <<EOF
MYSQL_URL=${MYSQL_URL}
NORIA_DEPLOYMENT=${DEPLOYMENT}
ZOOKEEPER_ADDRESS=${ZOOKEEPER_ADDRESS}
EOF
chmod 600 /etc/default/readyset-mysql-adapter

systemctl reset-failed
systemctl enable readyset-mysql-adapter
systemctl restart readyset-mysql-adapter
