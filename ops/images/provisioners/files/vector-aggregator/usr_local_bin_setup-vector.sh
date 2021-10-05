#!/usr/bin/env bash
set -euo pipefail

consul services register /etc/vector.d/vector.hcl
