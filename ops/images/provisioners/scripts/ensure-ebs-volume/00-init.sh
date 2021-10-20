#!/usr/bin/env bash
set -euxo pipefail

sudo install -o root -g root -m 755 \
    /tmp/ensure-ebs-volume-binary \
    /usr/local/bin/ensure-ebs-volume

ensure-ebs-volume --help # ensure it runs
