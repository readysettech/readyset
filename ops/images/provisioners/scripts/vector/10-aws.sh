#!/usr/bin/env bash
set -euxo pipefail

sudo install -o root -g root -m 755 \
    /tmp/vector/usr_local_bin_configure-vector.sh \
    /usr/local/bin/configure-vector.sh
