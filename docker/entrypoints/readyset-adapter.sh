#!/bin/bash
set -euf -x -o pipefail
shopt -s nullglob # have globs expand to nothing when they don't match

ENGINE="${ENGINE:-mysql}"
ADAPTER_ARGS="${*:---help}"

if [ "${ENGINE}" == "psql" ]; then
  echo "[INFO] Postgres mode selected. Starting readyset-psql ..."
  /usr/local/bin/readyset-psql ${ADAPTER_ARGS}
elif [ "${ENGINE}" == "mysql" ]; then
  echo "[INFO] MySQL mode selected. Starting readyset-mysql ..."
  /usr/local/bin/readyset-mysql ${ADAPTER_ARGS}
else
  echo "[ERROR] Unsupported DB engine specified as environment variable: ENGINE. Value is: ${ENGINE}"
  exit 1
fi
