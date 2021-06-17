#!/usr/bin/env bash
set -euo pipefail

echo -n "$BUILDKITE_BUILD_ID" > /tmp/latest-release-build
aws s3 cp /tmp/latest-release-build "s3://$DEPLOY_ARTIFACTS_BUCKET/latest-release-build"
