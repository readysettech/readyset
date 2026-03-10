#!/bin/bash
# init-replica.sh  — runs inside the replica container at first boot
# (placed in /docker-entrypoint-initdb.d/ so MySQL executes it after init)

set -euo pipefail

PRIMARY_DNS="mysql-primary"
PRIMARY_PORT=3306
REPL_USER="repl_user"
REPL_PASSWORD="repl_password"
ROOT_PASSWORD="noria"
echo "[replica-init] Waiting for primary to be ready..."
until mysql -h"$PRIMARY_DNS" -P"$PRIMARY_PORT" -uroot -p"$ROOT_PASSWORD" \
      -e "SELECT 1;" &>/dev/null; do
  echo "[replica-init] Primary not yet available — retrying in 3 s..."
  sleep 3
done
echo "[replica-init] Primary is up."

# Create the replication user on the PRIMARY (idempotent)
mysql -h"$PRIMARY_DNS" -P"$PRIMARY_PORT" -uroot -p"$ROOT_PASSWORD" <<-EOSQL
  CREATE USER IF NOT EXISTS '${REPL_USER}'@'%'
    IDENTIFIED WITH caching_sha2_password BY '${REPL_PASSWORD}';
  GRANT REPLICATION SLAVE ON *.* TO '${REPL_USER}'@'%';
  FLUSH PRIVILEGES;
EOSQL
echo "[replica-init] Replication user ensured on primary."

# Point this replica at the primary using GTID auto-positioning
mysql -uroot -p"$ROOT_PASSWORD" <<-EOSQL
  CHANGE REPLICATION SOURCE TO
    SOURCE_HOST     = '${PRIMARY_DNS}',
    SOURCE_PORT     = ${PRIMARY_PORT},
    SOURCE_USER     = '${REPL_USER}',
    SOURCE_PASSWORD = '${REPL_PASSWORD}',
    SOURCE_AUTO_POSITION = 1,
    GET_SOURCE_PUBLIC_KEY = 1;
  START REPLICA;
EOSQL
echo "[replica-init] Replication started."
