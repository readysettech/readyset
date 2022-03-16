#!/usr/bin/env bash
set -eux -o pipefail

sudo install -D -o vector -g vector -m 644 \
    /tmp/readyset-monitor/aws/etc_vector_vector.toml \
    /etc/vector/vector.toml

sudo install -D -o consul -g consul -m 644 \
    /tmp/readyset-monitor/aws/etc_consul.d_vector.hcl \
    /etc/consul.d/vector.hcl

sudo install -o root -g root -m 755 \
    /tmp/readyset-monitor/aws/usr_local_bin_user-data-init.sh \
    /usr/local/bin/user-data-init.sh

