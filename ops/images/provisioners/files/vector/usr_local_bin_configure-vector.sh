#!/usr/bin/env bash
set -euo pipefail

sudo install -d -o vector -p vector -m 664 /opt/vector

systemctl reset-failed
systemctl enable vector.service
systemctl restart vector.service
