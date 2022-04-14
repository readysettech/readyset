#!/bin/sh
set -eux

# Uploads installer docker images to S3

mkdir -p /tmp/docker-images
buildkite-agent artifact download readyset-*-release-${BUILDKITE_COMMIT}.tar.gz /tmp/docker-images
cd /tmp/docker-images
aws s3 cp . "s3://readysettech-orchestrator-us-east-2/docker-images/latest/" \
  --exclude "*" \
  --include "*.tar.gz" \
  --acl bucket-owner-full-control \
  --recursive