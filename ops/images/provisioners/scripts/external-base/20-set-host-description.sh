#!/bin/sh
set -eux

sudo install -o root -g root -m 755 \
  /tmp/external-base/aws/set-host-description.sh \
  /usr/local/bin/set-host-description.sh