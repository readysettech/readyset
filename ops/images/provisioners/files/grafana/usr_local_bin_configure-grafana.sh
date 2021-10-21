#!/usr/bin/env bash
set -euo pipefail

systemctl reset-failed
systemctl enable grafana-server.service
systemctl restart grafana-server.service
