#!/bin/bash
# Creates a kubernetes secret for the upstream database ReadySet will cache
# By default, assumes the mysql helm chart will be used
# Comment out the mysql or postgres RDS commands below as appropriate

# For MySQL using k8s
DB_USERNAME="root"
DB_PASSWORD="readyset"
DB_NAME="readyset"
# Assumes the database is generated with the setup_mysql.sh script. If not,
# adjust the connection string below accordingly
CONN_STRING="mysql://${DB_USERNAME}:${DB_PASSWORD}@rs-mysql.default.svc.cluster.local:3306/${DB_NAME}"

# For RDS, comment out the variables above and supply the ones below instead.

# DB_USERNAME="<db username>"
# DB_PASSWORD="<db password>"
# DB_NAME="<db name>"
# RDS_ENDPOINT="<rds endpoint>"


# For RDS MySQL
# CONN_STRING="mysql://${DB_USERNAME}:${DB_PASSWORD}@${RDS_ENDPOINT}:3306/${DB_NAME}"

# For RDS Postgres upstream databases:**
# CONN_STRING=postgresql://${DB_USERNAME}:${DB_PASSWORD}@${RDS_ENDPOINT}:5432/${DB_NAME}?sslmode=disable

kubectl create secret \
  generic \
  readyset-db-url \
  --from-literal=url="${CONN_STRING}" \
  --from-literal=username="${DB_USERNAME}" \
  --from-literal=database="${DB_NAME}" \
  --from-literal=password="${DB_PASSWORD}"
