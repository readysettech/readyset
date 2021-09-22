#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
    /tmp/setup-data-volume/usr_local_bin_setup-data-volume \
    /usr/local/bin/setup-data-volume
