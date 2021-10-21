#!/usr/bin/env bash
set -euo pipefail

systemctl reset-failed
systemctl enable prometheus.service
systemctl restart prometheus.service
