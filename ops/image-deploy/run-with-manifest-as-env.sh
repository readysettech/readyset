#!/bin/sh
set -eux

manifest_path=$1
shift
READYSET_AUTHORITY_CONSUL_AMI_ID=$(jq -r\
  '.builds[] | select(.name == "readyset-authority-consul") | .artifact_id | split(":")[1]'\
  "$manifest_path"
)
READYSET_MONITOR_AMI_ID=$(jq -r\
  '.builds[] | select(.name == "readyset-monitor") | .artifact_id | split(":")[1]'\
  "$manifest_path"
)
READYSET_MYSQL_ADAPTER_AMI_ID=$(jq -r\
  '.builds[] | select(.name == "readyset-mysql-adapter") | .artifact_id | split(":")[1]'\
  "$manifest_path"
)
READYSET_SERVER_AMI_ID=$(jq -r\
  '.builds[] | select(.name == "readyset-server") | .artifact_id | split(":")[1]'\
  "$manifest_path"
)
export READYSET_AUTHORITY_CONSUL_AMI_ID
export READYSET_MONITOR_AMI_ID
export READYSET_MYSQL_ADAPTER_AMI_ID
export READYSET_SERVER_AMI_ID
exec "$@"
