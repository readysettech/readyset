#!/bin/sh
set -eux

# Uploads the rendered CloudFormation template artifacts from Buildkite to the
# S3 Bucket and Prefix as defined by the environment variables S3_BUCKET and
# S3_PREFIX.

if [ -z ${S3_BUCKET+x} ]; then
  echo "S3_BUCKET not set"
  exit 1
fi

if [ -z ${S3_PREFIX+x} ]; then
  echo "S3_PREFIX not set"
  exit 1
fi

cd ops/cfn
mkdir ./rendered_templates
buildkite-agent artifact download "*.yaml" --step packer-cfn-template-ytt ./rendered_templates
cd ./rendered_templates
aws s3 cp . "s3://${S3_BUCKET}/${S3_PREFIX}/readyset/templates/" \
  --exclude "*" \
  --include "*.yaml" \
  --recursive
