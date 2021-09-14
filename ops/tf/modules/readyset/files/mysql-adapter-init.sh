#!/bin/sh

cat > /etc/default/readyset-mysql-adapter <<EOF
MYSQL_URL=${MYSQL_URL}
NORIA_DEPLOYMENT=${DEPLOYMENT}
AUTHORITY_ADDRESS=${AUTHORITY_ADDRESS}
AUTHORITY=${AUTHORITY}
EOF
chmod 600 /etc/default/readyset-mysql-adapter

systemctl reset-failed
systemctl enable readyset-mysql-adapter
systemctl restart readyset-mysql-adapter
