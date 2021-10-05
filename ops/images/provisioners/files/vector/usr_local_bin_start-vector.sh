#!/usr/bin/env bash
set -euo pipefail

/usr/local/bin/setup-vector.sh
/usr/local/bin/vector --config /etc/vector.d/vector.toml

