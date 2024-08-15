#!/usr/bin/env bash

set -exo pipefail

# Parse the upstream database URL
RE='(.+):\/\/([^:@]*)(:([^@]*))?@([^:\/]+)(:[0-9]+)?(\/(.+))?'
#   ^  ^^   ^^      ^^^^       ^^^       ^^        ^^       ^
#   |1 ||2  ||3     ||4|5      |||6      ||7       ||8      |
#   |  ||   ||      ||||       |||       ||        |└-------> Database name [optional]
#   |  ||   ||      ||||       |||       |└--------> Port [optional]
#   |  ||   ||      ||||       ||└-------> Host [required]
#   |  ||   ||      ||||       |└-> "@" character separator [required]
#   |  ||   ||      |||└-------> Password [optional]
#   |  ||   ||      |└> ":" separator [optional unless password is supplied]
#   |  ||   |└------> Username [required]
#   |  |└---> "://" Separator [required]
#   └--> Protocol [required] (e.g., postgresql, mysql)

# Only parse if UPSTREAM_DB_URL is not empty.
# Otherwise, these parameters must be passed down from the environment.
# TODO: Remove UPSTREAM_DB_URL completely when new docker images are in ECR
if [[! -z "${UPSTREAM_DB_URL}" ]; then
    [[ $UPSTREAM_DB_URL =~ $RE ]]
    RS_DB_TYPE=${BASH_REMATCH[1]}
    RS_USER=${BASH_REMATCH[2]}
    RS_PASS=${BASH_REMATCH[4]}
    RS_DB_NAME=${BASH_REMATCH[8]}
fi

# Rename RS_GRAFANA_PORT to GRAFANA_HTTP_PORT
if [ -z "${GRAFANA_HTTP_PORT}" ]; then
    GRAFANA_HTTP_PORT="$RS_GRAFANA_PORT"
fi

if [[ $RS_DB_TYPE = "mysql" ]]; then
    CONFIG_FILE=/etc/grafana/provisioning/datasources/default.template.mysql.yml
elif [[ $RS_DB_TYPE = "postgresql" || $RS_DB_TYPE = "postgres" ]]; then
    CONFIG_FILE=/etc/grafana/provisioning/datasources/default.template.postgres.yml
else
    echo "Unrecognized RS_DB_TYPE: $RS_DB_TYPE, exiting"
    exit 1
fi

# Fill in the config file templates
eval "cat <<EOF
$(</etc/grafana/grafana.template.ini)
EOF
" > /etc/grafana/grafana.ini
eval "cat <<EOF
$(<$CONFIG_FILE)
EOF
" > /etc/grafana/provisioning/datasources/default.yml

# Run the Grafana entrypoint
exec /run.sh "$@"
