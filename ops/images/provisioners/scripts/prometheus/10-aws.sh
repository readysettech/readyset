#!/usr/bin/env bash
set -euxo pipefail

sudo install -o root -g root -m 755 \
    /tmp/prometheus/usr_local_bin_configure-prometheus.sh \
    /usr/local/bin/configure-prometheus.sh
