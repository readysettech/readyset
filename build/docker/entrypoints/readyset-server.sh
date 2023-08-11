#!/bin/bash
set -euf -x -o pipefail
shopt -s nullglob # have globs expand to nothing when they don't match

SERVER_ARGS="${*:---help}"
#
# Consul Opts
CONSUL_ADDR="${AUTHORITY_ADDRESS:-127.0.0.1:8500}"
CONSUL_PROTO="${CONSUL_PROTO:-http}"
CONSUL_MAX_RETRY="${CONSUL_HEALTH_MAX_RETRY:-60}"
CONSUL_SLEEP="${CONSUL_HEALTH_SLEEP:-2}"

# Url to Consul cluster's leader status endpoint
_CONSUL_LEADER_URL="${CONSUL_PROTO}://${CONSUL_ADDR}/v1/status/leader"

# Ensure Consul has elected a leader before launching ReadySet.
# Avoids CrashLoopBackoff while Consul cannot be reached.
INIT_REQUIRE_LEADER="${INIT_REQUIRE_LEADER:-0}"

await_consul_leader_election () {
  local i=1
  result=1
  while [[ $i -le $CONSUL_MAX_RETRY ]]; do
    echo "Checking whether Consul leader elected [Attempt $i / $CONSUL_MAX_RETRY]"
    if curl --fail $_CONSUL_LEADER_URL; then
      # Leader has been elected.
      result=0
      break
    fi
    echo "Consul leader not elected. Sleeping $CONSUL_SLEEP seconds."
    i=$((i+1))
    sleep $CONSUL_SLEEP
  done

  if [ $result -gt 0 ]; then
    echo "No consul leader established after ${CONSUL_MAX_RETRY} retries. Exiting with error."
    exit 1
  fi
}

if [ "${INIT_REQUIRE_LEADER}" == "1" ]; then
  echo "[INFO] Checking status of Consul cluster leader election ..."
  await_consul_leader_election
fi

echo "[INFO] Starting ReadySet-Server ..."

/usr/local/bin/readyset-server ${SERVER_ARGS}
