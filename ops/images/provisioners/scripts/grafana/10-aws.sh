#!/usr/bin/env bash
set -euxo pipefail

sudo install -o root -g root -m 755 \
    /tmp/grafana/usr_local_bin_configure-grafana.sh \
    /usr/local/bin/configure-grafana.sh
