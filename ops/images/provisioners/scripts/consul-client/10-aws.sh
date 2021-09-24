#!/usr/bin/env bash
set -euxo pipefail

sudo install -o root -g root -m 755 \
    /tmp/consul-client/usr_local_bin_configure-consul-client.sh \
    /usr/local/bin/configure-consul-client.sh
