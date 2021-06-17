#!/usr/bin/env bash
set -euo pipefail

aws s3 cp "s3://$DEPLOY_ARTIFACTS_BUCKET/latest-release-build" /tmp/latest-release-build >&2
cat /tmp/latest-release-build
