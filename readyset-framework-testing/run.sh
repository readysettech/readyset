#!/bin/bash

dir="$(dirname "${0}")"

# usage: contains "$list" "$value"
#   returns success if $value is in $list; failure if not
function contains() {
  for item in $1; do
    if [ "$item" == "$2" ]; then
      return 0
    fi
  done
  return 1
}

function cleanup_tables() {
  mysql --host "${RS_HOST}" --port "${RS_PORT}" --user "${RS_USERNAME}" "-p${RS_PASSWORD}" "${RS_DATABASE}" -Ne 'SELECT DISTINCT TABLE_NAME, CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL' 2>/dev/null | while read -r table key; do
    echo "ALTER TABLE \`${table}\` DROP FOREIGN KEY \`${key}\`;";
  done | mysql --host "${RS_HOST}" --port "${RS_PORT}" --user "${RS_USERNAME}" "-p${RS_PASSWORD}" "${RS_DATABASE}" 2>/dev/null;

  tables="$(mysql --host "${RS_HOST}" --port "${RS_PORT}" --user "${RS_USERNAME}" "-p${RS_PASSWORD}" "${RS_DATABASE}" -Ne 'SHOW TABLES' 2>/dev/null | sed 's/^\|$/`/g' | xargs echo | sed 's/ /,/g')";
  if [[ "${tables}" != '' ]]; then
    mysql --host "${RS_HOST}" --port "${RS_PORT}" --user "${RS_USERNAME}" "-p${RS_PASSWORD}" "${RS_DATABASE}" -Ne "DROP TABLE ${tables}" 2>/dev/null;
  fi

  mysql --host "${RS_HOST}" --port "${RS_PORT}" --user "${RS_USERNAME}" "-p${RS_PASSWORD}" "${RS_DATABASE}" -Ne 'SHOW VSCHEMA TABLES' 2>/dev/null | while read -r table; do
    if [[ "$table" != "dual" ]]; then
      echo "ALTER VSCHEMA DROP TABLE \`${table}\`;"
    fi
  done | mysql --host "${RS_HOST}" --port "${RS_PORT}" --user "${RS_USERNAME}" "-p${RS_PASSWORD}" "${RS_DATABASE}" 2>/dev/null;
}

# usage: generate_image_name "$dialect" "$language/$framework"
function generate_image_name() {
  echo "305232526136.dkr.ecr.us-east-2.amazonaws.com/frameworks/${1}/${2}:latest" | tr '[:upper:]' '[:lower:]'
}

# usage: pull_image "$dialect" "$language/$framework"
function pull_image() {
  docker pull "$(generate_image_name "${1}" "${2}")"
}

# usage: build_image "$dialect" "$language/$framework"
function build_image() {
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
  docker build -t "$(generate_image_name "${1}" "${2}")" -f "$SCRIPT_DIR/frameworks/${2}/Dockerfile.${1}" "$SCRIPT_DIR/frameworks/${2}"
}

# usage: run_test language/framework
#    To rebuild a framework's container image for testing during local development, use build_image "$dialect" "$language/$framework"
function run_test() {
  validate_environment

  local dialect language framework
  language="$(echo "$1" | cut -d'/' -f1)"
  framework="$(echo "$1" | cut -d'/' -f2)"
  if [[ "${RS_DIALECT}" == 'mysql57' ]] || [[ "${RS_DIALECT}" == 'mysql80' ]]; then
    dialect=mysql
  fi

  pushd "frameworks/${language}/${framework}" >/dev/null || return

  tag="$(generate_image_name "${dialect}" "${language}/${framework}")"
  if [ -n "${QUIET}" ]; then
    docker run --rm -i -e RS_HOST -e RS_USERNAME -e RS_PASSWORD -e RS_PORT -e RS_DATABASE -e RS_NUM_SHARDS -e RS_DIALECT "${tag}" &>/dev/null
  else
    docker run --rm -i -e RS_HOST -e RS_USERNAME -e RS_PASSWORD -e RS_PORT -e RS_DATABASE -e RS_NUM_SHARDS -e RS_DIALECT "${tag}"
  fi;

  result="$?"
  echo "${dialect}/${language}/${framework}: $result"
  popd >/dev/null || return

  cleanup_tables
  return $result
}

function validate_environment() {
  if [[ -z "$RS_HOST" || -z "$RS_PORT" || -z "$RS_USERNAME" || -z "$RS_PASSWORD" || -z "$RS_DATABASE" || -z "$RS_NUM_SHARDS" || -z "$RS_DIALECT" ]]; then
    echo "Ensure RS_{HOST,PORT,USERNAME,PASSWORD,DATABASE,NUM_SHARDS,DIALECT} are set"
    exit 1
  fi
  if [[ "$RS_DIALECT" != 'mysql57' ]] && [[ "$RS_DIALECT" != 'mysql80' ]]; then
    echo "Unknown RS_DIALECT '$RS_DIALECT'; must be either 'mysql57' or 'mysql80'"
    exit 1
  fi
}

function get_frameworks() {
  find frameworks -mindepth 2 -maxdepth 2 -prune -type d | cut -d'/' -f2-
}

cmd="${1}"
shift

"${cmd}" "$@"

