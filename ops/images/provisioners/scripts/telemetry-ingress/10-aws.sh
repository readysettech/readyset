#!/usr/bin/env bash
set -eux -o pipefail

sudo install -o root -g root -m 755 \
  /tmp/telemetry-ingress/aws/usr_local_bin_user-data-init.sh \
  /usr/local/bin/user-data-init.sh
