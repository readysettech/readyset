#!/usr/bin/env bash
set -euo pipefail

/usr/bin/timeout 20 sh -c 'while ! ss -H -t -l -n sport = :9090 | grep -q "^LISTEN.*:9090"; do sleep 1; done'
