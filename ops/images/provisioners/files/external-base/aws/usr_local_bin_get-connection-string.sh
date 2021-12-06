#!/bin/sh
set -eux

get_ssm_by_path() {
  echo "$(aws ssm get-parameter --name $1 --with-decryption --output text --query Parameter.Value)"
}

if [ -z "${SSM_PATH_DB_PASSWORD}" ]; then
  echo "SSM_PATH_DB_PASSWORD not set. Unable to determine SSM path to DB password. Halting!"
  exit 1
fi

set +e
export PASSWORD="$(get_ssm_by_path ${SSM_PATH_DB_PASSWORD})"
set -e
if [ -z "${PASSWORD}" ]; then
  echo "Failed to find a usable value in parameter store path: ${SSM_PATH_DB_PASSWORD}. Halting!"
  exit 1
fi

# Building connection string
export DB_URL="${DB_PROTO}://${USERNAME}:${PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
